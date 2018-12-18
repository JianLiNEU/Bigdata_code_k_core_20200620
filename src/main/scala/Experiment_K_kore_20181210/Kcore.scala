package Experiment_K_kore_20181210

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.graphx._

import scala.collection.immutable.IntMap
/*
20181211
分布式求K-核
 */

object Kcore {
  def KcoreGraph(graph: Graph[Int, Int]): Graph[Int,Int] ={
    val initialMsg="-10"
    def mergeMsg(msg1: String, msg2:String): String = msg1+":"+msg2

    def vprog(vertexId: VertexId, value: (Int, Int,IntMap[Int],Int), message: String): (Int, Int,IntMap[Int],Int) = {
      if (message == initialMsg){
        return (value._1,value._2,value._3,value._4)
      }
      else{
        val msg=message.split(":")
        val elems=msg //newWeights.values
        var counts:Array[Int]=new Array[Int](value._1+1)
        for (m <-elems){
          val im=m.toInt
          if(im<=value._1)
          {
            counts(im)=counts(im)+1
          }
          else{
            counts(value._1)=counts(value._1)+1
          }
        }
        var curWeight =  0 //value._4-newWeights.size
        for(i<-value._1 to 1 by -1){
          curWeight=curWeight+counts(i)
          if(i<=curWeight){
            return (i, value._1,value._3,value._4)
          }
        }
        return (0, value._1,value._3,value._4)
      }
    }

    def sendMsg(triplet: EdgeTriplet[(Int, Int,IntMap[Int],Int), Int]): Iterator[(VertexId, String)] = {
      val sourceVertex = triplet.srcAttr
      val destVertex=triplet.dstAttr
      return Iterator((triplet.dstId,sourceVertex._1.toString),(triplet.srcId,destVertex._1.toString))

    }
      val maxIter=10
      val deg=graph.degrees

    val mgraph=graph.outerJoinVertices(deg)((id, oldattr, newattr) =>newattr.getOrElse(0)).mapVertices((id, attr) =>(attr,-1,IntMap[Int](),attr))
    graph.unpersist()
    val minGraph = mgraph.pregel(initialMsg,maxIter,EdgeDirection.Either)(vprog,sendMsg,mergeMsg)

    val kmax=minGraph.vertices.map(_._2._1).max
    val pruned=minGraph.subgraph(vpred=(id, attr) => attr._1==kmax)
    minGraph.vertices.foreach(u=>println(u._1+" "+u._2))
    val  KcoreGraph=minGraph.mapVertices((id,attr)=>attr._1)
    KcoreGraph.vertices.foreach(println(_))
    KcoreGraph


  }


}
