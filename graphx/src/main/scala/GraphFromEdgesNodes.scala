import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.graphx.GraphLoader
import scala.collection.mutable.ArrayBuffer
import org.apache.spark.graphx.Graph
import org.apache.spark.rdd.RDD
import org.apache.spark.graphx.VertexId
import org.apache.spark.storage.StorageLevel

import java.util.concurrent.TimeUnit
import java.nio.ByteBuffer
import java.nio.ByteOrder
import java.nio.file.{Files, Paths}
import java.io._


import org.apache.spark.graphx.impl.{EdgePartitionBuilder, GraphImpl}
import org.apache.spark.internal.Logging

object GraphFromEdgesNodes {

  def time[A](f: => A) = {
  val s = System.nanoTime
  val ret = f
  println("time: "+(System.nanoTime-s)/1e6+"ms")
  ret
}


  /**
   * Loads a graph from an edge list formatted file where each line contains two integers: a source
   * id and a target id. Skips lines that begin with `#`.
   *
   * If desired the edges can be automatically oriented in the positive
   * direction (source Id is less than target Id) by setting `canonicalOrientation` to
   * true.
   *
   * @example Loads a file in the following format:
   * {{{
   * # Comment Line
   * # Source Id <\t> Target Id
   * 1   -5
   * 1    2
   * 2    7
   * 1    8
   * }}}
   *
   * @param sc SparkContext
   * @param path the path to the file (e.g., /home/data/file or hdfs://file)
   * @param canonicalOrientation whether to orient edges in the positive
   *        direction
   * @param numEdgePartitions the number of partitions for the edge RDD
   * Setting this value to -1 will use the default parallelism.
   * @param edgeStorageLevel the desired storage level for the edge partitions
   * @param vertexStorageLevel the desired storage level for the vertex partitions
   */
  def loadEdgeList(
      sc: SparkContext,
      path: String,
      canonicalOrientation: Boolean = false,
      numEdgePartitions: Int = -1,
      edgeStorageLevel: StorageLevel = StorageLevel.MEMORY_ONLY,
      vertexStorageLevel: StorageLevel = StorageLevel.MEMORY_ONLY)
    : Graph[Int, Int] =
  {
    val startTimeNs = System.nanoTime()

    //val vertexBytes = sc.binaryRecords(path + ".nodes", 4);
    val vertexBytes = Files.readAllBytes(Paths.get(path + ".nodes"))
    val rawVertices = vertexBytes.grouped(4).map { element =>
      ByteBuffer.wrap(element).order(java.nio.ByteOrder.LITTLE_ENDIAN).getInt
    }

    val rawEdges = ArrayBuffer[(Long, Long)]()
    val dis = new DataInputStream(new BufferedInputStream(new FileInputStream(path + ".edges")))
    rawVertices.grouped(2).foreach { value =>
      println(s"${value(0)} ${value(1)}")
      val srcId = value(0)
      val count = value(1)

      for ( _ <- 1 to count) {
        val destId = java.lang.Integer.reverseBytes(dis.readInt())
        println(s"  ${destId}")
        val edge: (Long, Long) = (srcId, destId)
        rawEdges += edge
      }
    }
    println(rawEdges)
    /*
    rawVertices.grouped(2).foreach { (id, count) =>
      println(s"${id} ${count}")
    }
     */

/*
    // Parse the edge data table directly into edge partitions
    val lines =
      if (numEdgePartitions > 0) {
        sc.textFile(path, numEdgePartitions).coalesce(numEdgePartitions)
      } else {
        sc.textFile(path)
      }
    val edges = lines.mapPartitionsWithIndex { (pid, iter) =>
      val builder = new EdgePartitionBuilder[Int, Int]
      iter.foreach { line =>
        if (!line.isEmpty && line(0) != '#') {
          val lineArray = line.split("\\s+")
          if (lineArray.length < 2) {
            throw new IllegalArgumentException("Invalid line: " + line)
          }
          val srcId = lineArray(0).toLong
          val dstId = lineArray(1).toLong
          if (canonicalOrientation && srcId > dstId) {
            builder.add(dstId, srcId, 1)
          } else {
            builder.add(srcId, dstId, 1)
          }
        }
      }
      Iterator((pid, builder.toEdgePartition))
    }.persist(edgeStorageLevel).setName("GraphLoader.edgeListFile - edges (%s)".format(path))
    edges.count()
 */
    println(s"It took ${TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - startTimeNs)} ms" +
      " to load the edges")

    val edgesTuple = Array((1L,2L), (2L, 3L))
    val rdd = sc.parallelize(rawEdges)

    Graph.fromEdgeTuples(rdd, 1)
    /*
    GraphImpl.fromEdgePartitions(edges, defaultVertexAttr = 1, edgeStorageLevel = edgeStorageLevel,
      vertexStorageLevel = vertexStorageLevel)
     */
  } // end of edgeListFile

  def main(args: Array[String]) {

    //create SparkContext
    val sparkConf = new SparkConf().setAppName("GraphFromFile")
    val sc = new SparkContext(sparkConf)

    // read your file
    /*suppose your data is like
     v1 v3
     v2 v1
     v3 v4
     v4 v2
     v5 v3
     */
    val file = sc.textFile("/home/ronak/graphactors/data/test.txt");

    val graph = loadEdgeList(sc, args(0))
    // // create edge RDD of type RDD[(VertexId, VertexId)]
    // val edgesRDD: RDD[(VertexId, VertexId)] = file.map(line => line.split("\t"))
    //   .map(line =>
    //     (line(0).toInt, line(1).toInt))

    // // create a graph
    // val graph = Graph.fromEdgeTuples(edgesRDD, 1)

    // you can see your graph
    graph.triplets.collect.foreach(println)

    /*
    time {
    val ranks = graph.staticPageRank(20).vertices.sortBy(-_._2).take(1)
      println(ranks.foreach(println))
    }
     */
  }
}
