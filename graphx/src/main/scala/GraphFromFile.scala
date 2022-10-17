import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.graphx.GraphLoader
import scala.util.MurmurHash
import org.apache.spark.graphx.Graph
import org.apache.spark.rdd.RDD
import org.apache.spark.graphx.VertexId

object GraphFromFile {

  def time[A](f: => A) = {
  val s = System.nanoTime
  val ret = f
  println("time: "+(System.nanoTime-s)/1e6+"ms")
  ret
}

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

    val graph = GraphLoader.edgeListFile(sc, args(0))
    // // create edge RDD of type RDD[(VertexId, VertexId)]
    // val edgesRDD: RDD[(VertexId, VertexId)] = file.map(line => line.split("\t"))
    //   .map(line =>
    //     (line(0).toInt, line(1).toInt))

    // // create a graph
    // val graph = Graph.fromEdgeTuples(edgesRDD, 1)

    // you can see your graph
    //graph.triplets.collect.foreach(println)

    time {
    val ranks = graph.staticPageRank(20).vertices.sortBy(-_._2).take(1)
      println(ranks.foreach(println))
    }
  }
}
