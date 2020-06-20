package Experiment_K_kore_20181210

import org.apache.spark.graphx.{EdgeDirection, EdgeTriplet, Graph, VertexId}

import scala.collection.immutable.IntMap

object EffectGrdee {



  def EffectGraph(kcoreGraph: Graph[Int, Int]) = {
    val initialMsg="-10"
    def vprog(vertexId:VertexId, value:(Int,Int), message:String) :(Int,Int) = {

      var count=value._2
      if (message == initialMsg){
        return value
      }
      else {
        val msg = message.split(":")
        val elems = msg
        for (m <- elems) {
          if (m.toInt >= value._1) count+=1
        }
        return (value._1,count)
      }

    }

    def sendMsg(ET:EdgeTriplet[(Int,Int), Int] ):Iterator[(VertexId, String)] ={

      val sourceVertex = ET.srcAttr
      val destVertex=ET.dstAttr

      return Iterator((ET.dstId,sourceVertex._1.toString),(ET.srcId,destVertex._1.toString))
    }

    def mergeMsg(msg1:String, msg2:String) : String = {
      msg1+":"+msg2
    }



    val countGraph=kcoreGraph.mapVertices((id,attr)=>(attr,0))
    val minGraph = countGraph.pregel(initialMsg,1,EdgeDirection.Either)(vprog,sendMsg,mergeMsg)
    minGraph.vertices.foreach(println(_))
    minGraph
  }

}
