package Experiment_Kcore_Kdegree_20190316

import org.apache.spark.graphx.{Graph, VertexId}
import org.apache.spark.rdd.RDD

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

object CoreAnonyGraph {
  def anonygraph(PregelGraph: Graph[Map[Int, Array[Int]], Int]) = {
    val ArrayCandidate=ArrayBuffer[(Int,Int)]()
    var b=1

    val anonyPregelGraph=PregelGraph.mapVertices((id,attr)=>{
      b=1
      ArrayCandidate.clear()
      if(attr(id.toInt)(3)>0){//需要进行节点分裂

        ArrayCandidate+=((id.toInt+10000,id.toInt))
        if((attr(id.toInt)(3)-1)>0){
          attr.keys.foreach(a=>{
            if(attr(id.toInt)(3)-b>0&&attr(a)(2)>=attr(id.toInt)(2)&&attr(a)(1)!=id.toInt){
              ArrayCandidate+=((id.toInt+10000,attr(a)(1)))
              b=b+1
            }
          })
         ArrayCandidate
        }
        ArrayCandidate
      }
      else{
        ArrayBuffer()
      }
    })
    anonyPregelGraph
  }
}
