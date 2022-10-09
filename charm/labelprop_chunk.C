#include "labelprop_chunk.decl.h"
#include <stdio.h>

#include <fstream>
#include <iostream>
#include <sstream>
#include <string>

#include <algorithm>
#include <utility>

/*readonly*/ CProxy_Main mainProxy;
/*readonly*/ unsigned int numChunks;
/*readonly*/ unsigned int verticesPerChunk;

#define CHUNKINDEX(X) (std::min(numChunks - 1, X / verticesPerChunk))

constexpr int TRACE_PROPAGATE = 11;

/*mainchare*/
class Main : public CBase_Main
{
  private:
    double start;
    CProxy_Graph arrProxy;
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
        CkAbort("No chares per PE argument provided!");
      }

      numChunks = std::stoi(m->argv[2]) * CkNumPes();

      std::ifstream infile;
      infile.open(m->argv[1]);
      if (!infile)
        CkAbort("Could not open file %s", m->argv[1]);
      delete m;

      std::string line;
      unsigned int max = 0;

      while (std::getline(infile, line))
      {
        if (line[0] == '#')
          continue;
        std::stringstream ss(line);
        unsigned int src, dest;
        ss >> src;
        ss >> dest;

        if (src > max)
          max = src;
        if (dest > max)
          max = dest;
      }

      max += 1;
      verticesPerChunk = max / numChunks;
      // Start the computation
      CkPrintf("Running labelprop_chunk on with %d chunks, %d processors, %d "
               "nodes\n",
               numChunks, CkNumPes(), max);
      mainProxy = thisProxy;
      arrProxy = CProxy_Graph::ckNew(max, numChunks, numChunks);

      infile.clear();
      infile.seekg(0);

      while (std::getline(infile, line))
      {
        if (line[0] == '#')
          continue;
        std::stringstream ss(line);
        unsigned int src, dest;
        ss >> src;
        ss >> dest;

        // Assumes input is a directed graph, so convert to undirected
        if (src != dest)
        {
          arrProxy[CHUNKINDEX(src)].addEdge(std::make_pair(src, dest));
          arrProxy[CHUNKINDEX(dest)].addEdge(std::make_pair(dest, src));
        }
      }

      CkPrintf("Done adding edges\n");

      traceRegisterUserEvent("propagate", TRACE_PROPAGATE);

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
      CkPrintf("Graph created, %d total edges\n", count);
      traceBegin();
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
    std::vector<std::set<unsigned int>> edges;
    std::vector<unsigned int> labels;
    std::vector<unsigned int> oldLabels;
    std::vector<bool> fresh;

    unsigned int base;

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

    void addEdge(std::pair<unsigned int, unsigned int> edge)
    {
      const auto src = edge.first;
      const auto dest = edge.second;

      edges[src - base].insert(dest);
    }

    void getEdgeCount(CkCallback cb)
    {
      unsigned int count = 0;
      for (const auto& edgeSet : edges)
        count += edgeSet.size();
      contribute(sizeof(unsigned int), &count, CkReduction::sum_uint, cb);
    }

    void runlabelprop()
    {
      thisProxy.update();
    }

    void update()
    {
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
      if (thisIndex == 0)
      {
        CkPrintf("Iteration: fresh: %s\n", isFresh ? "true" : "false");
      }

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
        traceBeginUserBracketEvent(TRACE_PROPAGATE);
        for (int i = 0; i < fresh.size(); i++)
        {
          if (fresh[i])
          {
            for (const auto& dest : edges[i])
            {
              if (CHUNKINDEX(dest) == thisIndex)
              {
                propagate(std::make_pair(dest, labels[i]));
              }
              else
              {
                thisProxy[CHUNKINDEX(dest)].propagate(std::make_pair(dest, labels[i]));
              }
            }
          }
        }
        traceEndUserBracketEvent(TRACE_PROPAGATE);
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
};

#include "labelprop_chunk.def.h"
