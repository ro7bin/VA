import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.graphx._
import org.apache.spark.rdd.RDD
import sqlContext.createSchemaRDD

val sqlContext = new org.apache.spark.sql.SQLContext(sc)
import sqlContext.implicits._

val textFile =sc.textFile("/Users/robins/Downloads/violaexample-small/15-11-27-peers2.csv").coalesce(20)

case class Peer(val timestamp:String, val infohash:String, val ip:String, val port:String, val AS_number:String, val continent:String, val country:String, val city:String, val announceID:String)

val peers = textFile.map(_.split('\t')).
     filter(line => (line.length >8)).
     map(line=> new Peer(line(1).trim, line(2).trim, line(3).trim, line(4).trim, line(5).trim, line(6).trim, line(7).trim, line(8).trim, line(9).trim)).toDF()

peers.registerTempTable("peers")

	
val result = sqlContext.sql("select p.ip, q.ip, count(*) from peers as p, peers as q where p.infohash = q.infohash And p.ip < q.ip group by p.ip, q.ip").collect

result.foreach(println)

def pageHash(ip: String): VertexId = {
	ip.hashCode.toLong
	}
val resultRDD=sc.parallelize(result)

val edges: RDD[Edge[String]] = resultRDD.map { a =>
      val start = pageHash(a(0).toString)
      val end = pageHash(a(1).toString)
      Edge(start, end, a(2).toString)
      }
      
val graph : Graph[Any, String] = Graph.fromEdges(edges, "defaultProperty")
 println("num edges = " + graph.numEdges);
    println("num vertices = " + graph.numVertices);

