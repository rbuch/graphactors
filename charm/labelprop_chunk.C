#include "labelprop_chunk.decl.h"
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

      CkPrintf("Running labelprop_chunk on with %u chunks, %d processors, %u "
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

      std::vector<unsigned int> curDegs, curDests;
      unsigned int curChunk = 0, curBase = 0;
      while (nodeCursor < (unsigned int*)nodeFile + nodeLen)
      {
        unsigned int src, numEdges;
        src = *nodeCursor++;
        CmiEnforce(src < numVertices);
        numEdges = *nodeCursor++;

        maxVertex = std::max(maxVertex, src);

        const unsigned int chunk = CHUNKINDEX(src);
        if (chunk != curChunk)
        {
          arrProxy[curChunk].addAllEdges(std::move(curDegs), std::move(curDests));
          curDegs.clear();
          curDests.clear();
          curChunk = chunk;
          curBase = chunk * verticesPerChunk;
        }

        if (curDegs.size() < src - curBase)
          curDegs.resize(src - curBase);
        curDegs.push_back(numEdges);

        for (int i = 0; i < numEdges; i++)
        {
          const unsigned int dest = *edgeCursor++;
          CmiEnforce(dest < numVertices);
          curDests.push_back(dest);
          maxVertex = std::max(maxVertex, dest);
        }
      }

      if (!curDegs.empty())
      {
        arrProxy[curChunk].addAllEdges(curDegs, curDests);
        curDegs.clear();
        curDests.clear();
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
      arrProxy[0].runlabelprop();
    }

    void done(unsigned int count)
    {
      const auto end = CkWallTimer();
      CkPrintf("%d non-roots found in %fs\n", count, end - start);
      CkPrintf("All done\n");
      CkExit();
    };
};

/*array [1D]*/
class Graph : public CBase_Graph
{
  private:
    std::vector<std::vector<unsigned int>> edges;
    std::vector<unsigned int> labels;
    std::vector<unsigned int> oldLabels;
    std::vector<bool> fresh;

    unsigned int base;

    double start;

  public:
    Graph(int numVertices, int numElements)
        : base(thisIndex * (numVertices / numElements)) {
      CmiEnforce(numElements <= numVertices);
      // If this is the last chunk, fit the remainder in here
      if (thisIndex == numElements - 1)
        edges.resize(numVertices / numElements + (numVertices % numElements));
      else
        edges.resize(numVertices / numElements);

      // Assign labels to vertices
      labels.resize(edges.size());
      std::iota(labels.begin(), labels.end(), base);

      // Assign labels to vertices
      oldLabels.resize(edges.size());
      std::fill(oldLabels.begin(), oldLabels.end(),
                std::numeric_limits<unsigned int>::max());

      fresh.resize(edges.size());
    }

    Graph(CkMigrateMessage* m) {}

    void addEdge(std::pair<unsigned int, std::vector<unsigned int>> edgePair)
    {
      const auto src = edgePair.first;
      const auto dests = edgePair.second;

      edges[src - base] = dests;
    }

    void addAllEdges(std::vector<unsigned int> degs, std::vector<unsigned int> dests)
    {
      std::vector<std::vector<std::pair<unsigned int, unsigned int>>> revEdgeLists(numChunks);

      auto cursor = dests.begin();
      for (int i = 0; i < degs.size(); i++)
      {
        const auto src = i + base;
        for (int j = 0; j < degs[i]; j++)
        {
          const auto dest = *cursor++;
          if (dest == src)
            continue;
          edges[i].push_back(dest);
          if (CHUNKINDEX(dest) == thisIndex)
            edges[dest - base].push_back(src);
          else
            revEdgeLists[CHUNKINDEX(dest)].push_back(std::make_pair(dest, src));
        }
      }

      for (int i = 0; i < numChunks; i++)
      {
        thisProxy[i].recvReverse(revEdgeLists[i]);
      }
    }

    void recvReverse(std::vector<std::pair<unsigned int, unsigned int>> revEdges)
    {
      for (const auto& revEdge : revEdges)
      {
        const auto src = revEdge.first;
        const auto dest = revEdge.second;
        auto& srcEdges = edges[src - base];
        srcEdges.push_back(dest);
      }
    }

    void getEdgeCount(CkCallback cb)
    {
      unsigned int count = 0;
      for (auto& edgeVec : edges)
      {
        std::sort(edgeVec.begin(), edgeVec.end());
        const auto last = std::unique(edgeVec.begin(), edgeVec.end());
        edgeVec.erase(last, edgeVec.end());
        count += edgeVec.size();
      }
      contribute(sizeof(unsigned int), &count, CkReduction::sum_uint, cb);
    }

    void runlabelprop()
    {
      start = CkWallTimer();
      thisProxy.update();
    }

    void update()
    {
      if (thisIndex == 0)
      {
        CkPrintf("Iteration: %f\n", CkWallTimer() - start);
      }

      bool localFresh = false;
      for (int i = 0; i < labels.size(); i++)
      {
        fresh[i] = (labels[i] < oldLabels[i]);
        localFresh |= fresh[i];
      }
      oldLabels = labels;

      CkCallback cb(CkReductionTarget(Graph, iterate), thisProxy);
      contribute(sizeof(bool), &localFresh, CkReduction::logical_or_bool, cb);
    }

    void iterate(bool isFresh)
    {
      if (!isFresh)
      {
        unsigned int nonroot = 0;
        for (int i = 0; i < labels.size(); i++)
          nonroot += (labels[i] == i + base) ? 0 : 1;
        CkCallback cb(CkReductionTarget(Main, done), mainProxy);
        contribute(sizeof(unsigned int), &nonroot, CkReduction::sum_uint, cb);
      }
      else
      {
        std::vector<std::vector<std::pair<unsigned int, unsigned int>>> outgoing;
        outgoing.resize(numChunks);
        for (int i = 0; i < fresh.size(); i++)
        {
          if (fresh[i])
          {
            for (const auto& dest : edges[i])
            {
              if (CHUNKINDEX(dest) == thisIndex)
                propagate(std::make_pair(dest, labels[i]));
              else
              {
                const auto pair = std::make_pair(dest, labels[i]);
                outgoing[CHUNKINDEX(dest)].push_back(pair);
              }
            }
          }
        }

        for (int i = 0; i < outgoing.size(); i++)
        {
          thisProxy[i].propagateBatch(outgoing[i]);
        }

        if (thisIndex == 0)
        {
          CkCallback cb(CkIndex_Graph::update(), thisProxy);
          CkStartQD(cb);
        }
      }
    }

    void propagate(std::pair<unsigned int, unsigned int> candidate)
    {
      const auto dest = candidate.first;
      const auto newLabel = candidate.second;

      if (newLabel < labels[dest - base])
      {
        labels[dest - base] = newLabel;
      }
    }

    void propagateBatch(std::vector<std::pair<unsigned int, unsigned int>> candidates)
    {
      for (const auto& candidate : candidates)
      {
        const auto dest = candidate.first;
        const auto newLabel = candidate.second;

        if (newLabel < labels[dest - base])
        {
          labels[dest - base] = newLabel;
        }
      }
    }
};

#include "labelprop_chunk.def.h"
