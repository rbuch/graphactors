#include "pagerank_chunk.decl.h"
#include <iterator>
#include <stdio.h>

#include <fstream>
#include <iostream>
#include <sstream>
#include <string>

#include <sys/types.h>
#include <sys/mman.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <unistd.h>

#include <algorithm>
#include <utility>
#include <filesystem>
#include <tuple>

/*readonly*/ CProxy_Main mainProxy;
/*readonly*/ unsigned int numChunks;
/*readonly*/ unsigned int verticesPerChunk;

#define CHUNKINDEX(X) (std::min(numChunks - 1, X / verticesPerChunk))

/*mainchare*/
class Main : public CBase_Main
{
  private:
    double start;
    CProxy_Graph arrProxy;

    std::tuple<const char*, int, size_t> mapFile(std::filesystem::path p)
    {
      int fd = open(p.c_str(), O_RDONLY);
      if (fd == -1)
      {
        CkAbort("Could not open file %s\n", p.c_str());
      }

      struct stat stats;
      if (fstat(fd, &stats) == -1)
      {
        CkAbort("Could not stat fd %d\n", fd);
      }

      const size_t fsize = stats.st_size;

      const char* mappedFile =
          static_cast<const char*>(mmap(nullptr, fsize, PROT_READ, MAP_PRIVATE, fd, 0));
      if (mappedFile == MAP_FAILED)
      {
        CkAbort("Could not map fd %d\n", fd);
      }

      return std::make_tuple(mappedFile, fd, fsize);
    }
  public:
    Main(CkArgMsg* m)
    {
      // Process command-line arguments
      if (m->argc <= 1)
      {
        CkAbort("No file argument provided!");
      }

      if (m->argc <= 2)
      {
        CkAbort("No numVertices argument provided");
      }

      if (m->argc <= 3)
      {
        CkPrintf("No chares per PE argument provided, defaulting to 1!\n");
      }

      unsigned int numVertices = std::stoul(m->argv[2]);

      const unsigned int chunksPerPE = (m->argc <= 3) ? 1 : std::stoul(m->argv[3]);
      numChunks = chunksPerPE * CkNumPes();

      verticesPerChunk = numVertices / numChunks;

      CkPrintf("Running pagerank_chunk on with %u chunks, %d processors, %u "
               "vertices\n",
               numChunks, CkNumPes(), numVertices);
      mainProxy = thisProxy;
      arrProxy = CProxy_Graph::ckNew(numVertices, numChunks, numChunks);


      std::filesystem::path p(m->argv[1]);

      const char *nodeFile, *edgeFile;
      int nodeFd, edgeFd;
      size_t nodeFSize, edgeFSize;

      std::tie(nodeFile, nodeFd, nodeFSize) = mapFile(p.replace_extension(".nodes"));
      std::tie(edgeFile, edgeFd, edgeFSize) = mapFile(p.replace_extension(".edges"));

      const auto nodeLen = nodeFSize / sizeof(unsigned int);
      auto nodeCursor = (unsigned int*)nodeFile;
      auto edgeCursor = (unsigned int*)edgeFile;
      unsigned int maxVertex = 0;

      while (nodeCursor < (unsigned int*)nodeFile + nodeLen)
      {
        unsigned int src, numEdges;
        src = *nodeCursor++;
        numEdges = *nodeCursor++;

        maxVertex = std::max(src, maxVertex);

        std::vector<unsigned int> edges;
        edges.reserve(numEdges);

        for (int i = 0; i < numEdges; i++)
        {
          unsigned int dest = *edgeCursor++;
          maxVertex = std::max(dest, maxVertex);
          CmiEnforce(dest < numVertices && src < numVertices);
          edges.push_back(dest);
        }

        arrProxy[CHUNKINDEX(src)].addEdge(std::make_pair(src, edges));
      }
      munmap((void*)nodeFile, nodeFSize);
      munmap((void*)edgeFile, edgeFSize);
      close(nodeFd);
      close(edgeFd);

      CkPrintf("Done adding edges, found %u vertices\n", maxVertex + 1);
      CkCallback initCB(CkIndex_Main::initDone(), thisProxy);
      CkStartQD(initCB);
    };

    void initDone(void)
    {
      CkCallback cb(CkReductionTarget(Main, startComputation), thisProxy);
      arrProxy.getEdgeCount(cb);
    }

    void startComputation(unsigned int count)
    {
      CkPrintf("Graph created, %u total edges\n", count);
      start = CkWallTimer();
      arrProxy[0].runpagerank(0.85, 20);
    }

    void done(float maxVal)
    {
      const auto end = CkWallTimer();
      CkPrintf("All done, Max Val: %f\n", maxVal);
      CkExit();
    };
};

/*array [1D]*/
class Graph : public CBase_Graph
{
  private:
    std::vector<std::vector<unsigned int>> edges;
    std::vector<float> a, b;

    std::vector<unsigned int> vertexDegs;
    std::vector<unsigned int> compressedEdges;

    unsigned int base;

    std::vector<std::vector<std::pair<unsigned int, float>>> outgoing;

  public:
    Graph(int numVertices, int numElements)
        : base(thisIndex * (numVertices / numElements)) {
      CmiEnforce(numElements <= numVertices);
      // If this is the last chunk, fit the remainder in here
      if (thisIndex == numElements - 1)
        edges.resize(numVertices / numElements + (numVertices % numElements));
      else
        edges.resize(numVertices / numElements);

      outgoing.resize(numElements);

      a.resize(edges.size());
      std::fill(a.begin(), a.end(), 0);
      b.resize(edges.size());
      std::fill(b.begin(), b.end(), 0);

      usesAtSync = true;
      // Don't migrate index 0 since the threaded runpagerank EP shouldn't move
      if (thisIndex == 0)
        setMigratable(false);
    }

    Graph(CkMigrateMessage* m) {}

    void pup(PUP::er &p)
    {
      p | vertexDegs;
      p | compressedEdges;
      p | a;
      p | b;
      p | base;

      if (p.isUnpacking())
        outgoing.resize(numChunks);
    }

    void addEdge(std::pair<unsigned int, std::vector<unsigned int>> edgePair)
    {
      const auto src = edgePair.first;
      const auto dests = edgePair.second;

      edges[src - base] = dests;
    }

    void getEdgeCount(CkCallback cb)
    {
      vertexDegs.reserve(edges.size());
      for (int i = 0; i < edges.size(); i++)
      {
        // Pack the edge data into a single vector, vertexDegs[i] is the out
        // degree of vertex base + i, or the number of edges in compressedEdges
        // corresponding to it. Doing this greatly speeds up iterate()
        vertexDegs.emplace_back(edges[i].size());
        compressedEdges.insert(compressedEdges.end(),
                               std::make_move_iterator(edges[i].begin()),
                               std::make_move_iterator(edges[i].end()));
        edges[i].clear();
      }

      edges.clear();

      unsigned int count = compressedEdges.size();
      contribute(sizeof(unsigned int), &count, CkReduction::sum_uint, cb);
    }

    void runpagerank(float alpha, int iters)
    {
      const auto start = CkWallTimer();
      for (int i = 0; i < iters; i++)
      {
        thisProxy.update(alpha);
        CkWaitQD();
        thisProxy.iterate();
        CkWaitQD();
        const auto elapsed = CkWallTimer() - start;
        CkPrintf("Iteration %d:\t%f\n", i, elapsed);
        if (i == 0)
        {
          thisProxy.callAtSync();
          CkWaitQD();
        }
      }

      thisProxy.returnResults();
    }

    void update(float alpha)
    {
      for (int i = 0; i < vertexDegs.size(); i++)
      {
       b[i] = alpha * a[i] / vertexDegs[i];
       a[i] = 1 - alpha;
      }
    }

    void iterate()
    {
      auto edgeIt = compressedEdges.begin();
      for (int i = 0; i < vertexDegs.size(); i++)
      {
        const auto curB = b[i];
        for (int j = 0; j < vertexDegs[i]; j++)
        {
          const auto dest = *edgeIt++;
          if (numChunks > 1 && CHUNKINDEX(dest) != thisIndex)
            outgoing[CHUNKINDEX(dest)].push_back(std::make_pair(dest, curB));
          else
            addB(std::make_pair(dest, curB));
        }
      }

      for (int i = 0; i < outgoing.size(); i++)
      {
        if (!outgoing[i].empty())
        {
          thisProxy[i].addB(outgoing[i]);
          outgoing[i].clear();
        }
      }
    }

    void addB(const std::pair<unsigned int, float> b_in)
    {
      const auto dest = b_in.first;
      const auto value = b_in.second;
      a[dest - base] += value;
    }

    void addB(const std::vector<std::pair<unsigned int, float>> b_in)
    {
      for (const auto& entry : b_in)
      {
        addB(entry);
      }
    }

    void returnResults()
    {
      CkCallback cb(CkReductionTarget(Main, done), mainProxy);
      const float max = *std::max_element(a.begin(), a.end());
      contribute(sizeof(float), &max, CkReduction::max_float, cb);
    }

    void callAtSync()
    {
      AtSync();
    }
};

#include "pagerank_chunk.def.h"
