#include "pagerank.decl.h"
#include <stdio.h>

#include <fstream>
#include <iostream>
#include <sstream>
#include <string>

/*readonly*/ CProxy_Main mainProxy;

/*mainchare*/
class Main : public CBase_Main
{
private:
  CProxy_Graph arrProxy;
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
    int max = 0;

    while (std::getline(infile, line))
    {
      if (line[0] == '#')
        continue;
      std::stringstream ss(line);
      int src, dest;
      ss >> src;
      ss >> dest;

      if (src > max)
        max = src;
      if (dest > max)
        max = dest;
    }

    max += 1;

    // Start the computation
    CkPrintf("Running pagerank on %d processors, %d nodes\n", CkNumPes(), max);
    mainProxy = thisProxy;
    arrProxy = CProxy_Graph::ckNew(max);

    infile.clear();
    infile.seekg(0);

    while (std::getline(infile, line))
    {
      if (line[0] == '#')
        continue;
      std::stringstream ss(line);
      int src, dest;
      ss >> src;
      ss >> dest;

      arrProxy[src].addEdge(dest);
    }
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
    arrProxy[0].runpagerank(0.85, 20);
  }

  void done(float maxVal)
  {
    CkPrintf("All done, Max Val: %f\n", maxVal);
    CkExit();
  };
};

/*array [1D]*/
class Graph : public CBase_Graph
{
private:
  std::vector<int> edges;
  float a = 0, b = 0;
  int d = 0;

public:
    Graph() { }

  Graph(CkMigrateMessage* m) {}

  void addEdge(int dest)
  {
    edges.push_back(dest);
    d++;
  }

    void getEdgeCount(CkCallback cb)
    {
      unsigned int count = edges.size();
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
    }

    thisProxy.returnResults();
  }

  void update(float alpha)
  {
    b = alpha * a / d;
    a = 1 - alpha;
  }

  void iterate()
  {
    for (int edge : edges)
    {
      thisProxy[edge].addB(b);
    }
  }

  void addB(float b_in) { a += b_in; }

  void returnResults()
  {
    CkCallback cb(CkReductionTarget(Main, done), mainProxy);
    const float max = a;
    contribute(sizeof(float), &max, CkReduction::max_float, cb);
  }

};

#include "pagerank.def.h"
