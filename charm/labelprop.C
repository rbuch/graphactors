#include "labelprop.decl.h"
#include <stdio.h>

#include <fstream>
#include <iostream>
#include <sstream>
#include <string>

/*readonly*/ CProxy_Main mainProxy;
/*readonly*/ CProxy_Graph arrProxy;

/*mainchare*/
class Main : public CBase_Main
{
  private:
    double start;
  public:
    Main(CkArgMsg* m)
    {
      // Process command-line arguments
      if (m->argc <= 1)
      {
        CkAbort("No file argument provided!");
      }

      std::ifstream infile;
      infile.open(m->argv[1]);
      if (!infile)
        CkAbort("Could not open file %s", m->argv[1]);
      delete m;

      std::string line;
      uint64_t max = 0;

      while (std::getline(infile, line))
      {
        if (line[0] == '#')
          continue;
        std::stringstream ss(line);
        uint64_t src, dest;
        ss >> src;
        ss >> dest;

        if (src > max)
          max = src;
        if (dest > max)
          max = dest;
      }

      max += 1;

      // Start the computation
      CkPrintf("Running labelprop on %d processors, %ld nodes\n", CkNumPes(), max);
      mainProxy = thisProxy;
      arrProxy = CProxy_Graph::ckNew(max);

      infile.clear();
      infile.seekg(0);

      while (std::getline(infile, line))
      {
        if (line[0] == '#')
          continue;
        std::stringstream ss(line);
        uint64_t src, dest;
        ss >> src;
        ss >> dest;

        // Assumes input is a directed graph, so convert to undirected
        arrProxy[src].addEdge(dest);
        arrProxy[dest].addEdge(src);
      }

      CkPrintf("Done adding edges\n");
      CkCallback initCB(CkIndex_Main::initDone(), thisProxy);
      CkStartQD(initCB);
    };

    void initDone(void)
    {
      CkPrintf("Graph created!\n");
      start = CkWallTimer();
      arrProxy[0].runlabelprop();
    }

    void done(uint64_t count)
    {
      const auto end = CkWallTimer();
      CkPrintf("%ld non-roots found in %fs\n", count, end - start);
      CkPrintf("All done\n");
      CkExit();
    };
};

/*array [1D]*/
class Graph : public CBase_Graph
{
  private:
    std::set<uint64_t> edges;
    uint64_t label = thisIndex;
    uint64_t oldLabel = std::numeric_limits<uint64_t>::max();
    bool fresh;

  public:
    Graph() {}

    Graph(CkMigrateMessage* m) {}

    void addEdge(uint64_t dest)
    {
      edges.insert(dest);
    }

    void runlabelprop()
    {
      thisProxy.update();
    }

    void update()
    {
      fresh = (label < oldLabel);
      oldLabel = label;

      CkCallback cb(CkReductionTarget(Graph, iterate), thisProxy);
      contribute(sizeof(bool), &fresh, CkReduction::logical_or_bool, cb);
    }

    void iterate(bool isFresh)
    {
      if (thisIndex == 0)
      {
        CkPrintf("Iteration: fresh: %s\n", isFresh ? "true" : "false");
      }

      if (!isFresh)
      {
        unsigned long long nonroot = (label == thisIndex) ? 0 : 1;
        CkCallback cb(CkReductionTarget(Main, done), mainProxy);
        contribute(sizeof(unsigned long long), &nonroot, CkReduction::sum_ulong_long, cb);
      }
      else
      {
        if (fresh)
        {
          for (uint64_t dest : edges)
          {
            thisProxy[dest].propagate(label);
          }
        }
        if (thisIndex == 0)
        {
          CkCallback cb(CkReductionTarget(Graph, update), thisProxy);
          CkStartQD(cb);
        }
      }
    }

    void propagate(uint64_t candidateLabel)
    {
      if (candidateLabel < label)
      {
        label = candidateLabel;
      }
    }
};

#include "labelprop.def.h"
