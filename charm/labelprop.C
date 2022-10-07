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

      // Start the computation
      CkPrintf("Running labelprop on %d processors, %d nodes\n", CkNumPes(), max);
      mainProxy = thisProxy;
      arrProxy = CProxy_Graph::ckNew(max);

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
    std::set<unsigned int> edges;
    unsigned int label = thisIndex;
    unsigned int oldLabel = std::numeric_limits<unsigned int>::max();
    bool fresh;

  public:
    Graph() {}

    Graph(CkMigrateMessage* m) {}

    void addEdge(unsigned int dest)
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
        unsigned int nonroot = (label == thisIndex) ? 0 : 1;
        CkCallback cb(CkReductionTarget(Main, done), mainProxy);
        contribute(sizeof(unsigned int), &nonroot, CkReduction::sum_uint, cb);
      }
      else
      {
        if (fresh)
        {
          for (unsigned int dest : edges)
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

    void propagate(unsigned int candidateLabel)
    {
      if (candidateLabel < label)
      {
        label = candidateLabel;
      }
    }
};

#include "labelprop.def.h"
