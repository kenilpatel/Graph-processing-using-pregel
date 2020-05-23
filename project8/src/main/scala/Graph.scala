import org.apache.spark.graphx.{Graph,VertexId,Edge}
import org.apache.spark.graphx.util.GraphGenerators
import org.apache.spark.SparkContext
import org.apache.spark.SparkConf

object GraphComponents {
  def main ( args: Array[String] ) 
  {
  	val conf = new SparkConf().setAppName("GraphComponents")
  	val sc = new SparkContext(conf) 
  	var graph=sc.textFile(args(0)).map(line=> {val a=line.split(",")
     var adj=List[Int]()
     var vid=a(0).toInt
     a.tail.foreach(x=>adj=x.toInt::adj)
     (vid,vid,adj)})
  	var e=graph.map(x=>(x._3.map(y=>(y,x._2)))).map(x=>(x)).flatMap(x=>x)  
  	var edges=e.map(x=>new Edge(x._1,x._2,1)) 
  	var graph_build = Graph.fromEdges(edges,scala.Int.MaxValue)
  	var new_graph=graph_build.mapVertices((id,_)=>id)  
	val grp = new_graph.pregel(scala.Int.MaxValue,5)(
	      (id, dist, newDist) => math.min(dist, newDist),  
	      triplet => {   
	        if (triplet.srcAttr  <= triplet.dstAttr) {
	          Iterator((triplet.dstId, triplet.srcAttr.toInt))
	        } else {
	          Iterator.empty
	        }
	      },
	      (a, b) => math.min(a,b)  
	    )
	var vertex_group=grp.vertices.map({case (a,b) => (b,1)}) 
	vertex_group.reduceByKey(_+_).sortByKey().collect().foreach(x=>println(x._1+"\t"+x._2))
  }
}