package Experiment_Fire_pageRank

import org.apache.spark.SparkContext
import org.apache.spark.graphx.{EdgeDirection, EdgeTriplet, Graph, VertexId}

import scala.collection.mutable.ArrayBuffer
import scala.util.Random

object graphAnonymity {



  //###########################################################################################################
//寻找邻居节点
  def vprog(vid:VertexId, value:(VertexId, VertexState), message:Array[Int]) : (VertexId, VertexState) = {
    if(message.contains(0))value
    else {
      val newValue=new VertexState()
      newValue.neighborArray++=Map(vid.toInt->message)
      (vid,newValue)
    }
  }
  def senxdMsg(ET: EdgeTriplet[(VertexId, VertexState), Double]): Iterator[(VertexId, Array[Int])] = {
    Iterator((ET.srcId ,Array(ET.dstId.toInt)),(ET.dstId,Array(ET.srcId.toInt)))
  }
  def mergeMsg(A1:Array[Int], A2:Array[Int]) : Array[Int] = {
    A1++A2
  }
  //##############################################################################################

  //##############################################################################################
  //发送邻居节点
  def vprog01(vid:VertexId, value:(VertexId, VertexState), message:Map[Int, Array[Int]]) : (VertexId, VertexState) = {
    if(message.keySet.contains(0))value
    else{
      var newValue=new VertexState()

      newValue.neighborArray++=value._2.neighborArray
      newValue.neighborArray++=message
      (vid,newValue)
    }
  }
  def senxdMsg01(ET:EdgeTriplet[(VertexId, VertexState), Double] ):Iterator[(VertexId, Map[Int, Array[Int]])] = {
    Iterator((ET.dstId,ET.srcAttr._2.neighborArray))
  }
  def mergeMsg01(A1:Map[Int, Array[Int]], A2:Map[Int, Array[Int]]) :Map[Int, Array[Int]] = {
    A1++A2
  }
  //##############################################################################################################


  def anonymity(resultGraph: Graph[(VertexId, VertexState), Double], sc: SparkContext, p:Int)={
    //将同一社区的节点分组
    val initGroupValue=resultGraph.vertices.values.map((attr)=>(attr._2.community,attr._1)).groupByKey()
    //将同一组社区的值map到节点上
    val candidateVertice=resultGraph.vertices.map(attr=>{(attr._2._2.community,attr._1)}).leftOuterJoin(initGroupValue).map(attr=>{
      val state=new VertexState
      val array=attr._2._2.getOrElse(Iterable(0))
      state.arrayCandidate++=array
      (attr._2._1,state)
    })
    //收集节点的一邻居
    val pregelGraph=resultGraph.pregel(Array(0),1,EdgeDirection.Either)(vprog,senxdMsg,mergeMsg)
    val pregel01Graph=pregelGraph.pregel(Map(0->Array(0)),1,EdgeDirection.Either)(vprog01,senxdMsg01,mergeMsg01)

    val candidateNerVertice=pregel01Graph.vertices.leftOuterJoin(candidateVertice).mapValues(attr=>{
      val nodevalue=attr._1._2
      val state=new VertexState
      val nodeNeighbor=attr._2.getOrElse(state)
      nodevalue.arrayCandidate++=nodeNeighbor.arrayCandidate
      nodevalue
    })
    val candidateGraph=Graph(candidateNerVertice,resultGraph.edges)

    val random=new Random()
    println(candidateGraph.edges.collect().size)
    val lastresult=candidateGraph.mapVertices((id,vt)=>{
      var candidate=""//结果String
      var candidateArray=ArrayBuffer[Any]()//扰动后节点，防止多次扰动
      vt.neighborArray.keys.foreach(key=>{
        if(key!=id.toInt) {
          if (random.nextInt(100) > p) {
            //不扰动
            candidate += key + " " + id + "   "
          }
          else {
            vt.arrayCandidate --= candidateArray//防止多次扰动
            val removeIdAttr = vt.arrayCandidate -- vt.neighborArray(id.toInt)
            removeIdAttr--=Array(id.toInt)
            val removeKeyAttr = vt.arrayCandidate -- vt.neighborArray(key)
            removeKeyAttr--=Array(key)
            if (removeIdAttr.size > 0) {

              val candidateNode = removeIdAttr(random.nextInt(removeIdAttr.size))
              candidateArray += candidateNode
              candidate += candidateNode + " " + id + "   "
            }
            else if (removeKeyAttr.size > 0) {
              val candidateNode = removeKeyAttr(random.nextInt(removeKeyAttr.size))
              candidateArray += candidateNode
              candidate += key + " " + candidateNode + "   "
            }
            else {
              candidate += key + " " + id + "   "
            }
          }
        }

      })
      candidate
    })
    lastresult.vertices.map(_._2)
  }
//########################################################################################
  //第二、三次随机扰动，寻找节点邻居
  def vprog0203 (vid:VertexId,value:  VertexState,message: Array[Int]) : VertexState = {
    if(message.contains(0))value
    else {
      val newValue=new VertexState()
      newValue.neighborArray++=Map(vid.toInt->message)
      newValue
    }
  }

  def senxdMsg0203(ET:EdgeTriplet[VertexState, Int]): Iterator[(VertexId, Array[Int])] = {
    Iterator((ET.srcId ,Array(ET.dstId.toInt)),(ET.dstId,Array(ET.srcId.toInt)))
  }

  def mergeMsg0203(A1:Array[Int],A2: Array[Int]): Array[Int] = {
    A1++A2
  }
  //####################################################################################
  //####################################################################################
  //第二三次扰动，将节点邻居发送给目地节点
  def vprog010203(vid:VertexId, value:VertexState, message:Map[Int, Array[Int]]) : VertexState = {
    if(message.keySet.contains(0))value
    else{
      var newValue=new VertexState()

      newValue.neighborArray++=value.neighborArray
      newValue.neighborArray++=message
      newValue
    }
  }

  def senxdMsg010203(ET:EdgeTriplet[VertexState, Int] ): Iterator[(VertexId, Map[Int, Array[Int]])] = {
    Iterator((ET.dstId,ET.srcAttr.neighborArray))
  }

  def mergeMsg010203(A1:Map[Int, Array[Int]], A2:Map[Int, Array[Int]]): Map[Int, Array[Int]] = {
     A1++A2
  }
  //##########################################################################################

  //#################################第二次邻居与第三次邻居
  def anonymity0203(resultGraph: Graph[VertexState, Int], sc: SparkContext, p: Int) = {
    //将同一社区的节点分组

    val initGroupValue=resultGraph.vertices.map(attr=>(attr._2.community,attr._1)).groupByKey()
    //将同一组社区的值map到节点上
    val candidateVertice=resultGraph.vertices.map(attr=>{(attr._2.community,attr._1)}).leftOuterJoin(initGroupValue).map(attr=>{
      val state=new VertexState
      val array=attr._2._2.getOrElse(Iterable(0))
      state.arrayCandidate++=array
      (attr._2._1,state)
    })
    //收集节点的一邻居
    val pregelGraph=resultGraph.pregel(Array(0),1,EdgeDirection.Either)(vprog0203,senxdMsg0203,mergeMsg0203)
    //将1节点的邻居信息发送到目的节点。
    val pregel01Graph=pregelGraph.pregel(Map(0->Array(0)),1,EdgeDirection.Either)(vprog010203,senxdMsg010203,mergeMsg010203)

     val candidateNerVertice=pregel01Graph.vertices.leftOuterJoin(candidateVertice).mapValues(attr=>{
      val nodevalue=attr._1
      val state=new VertexState
      val nodeNeighbor=attr._2.getOrElse(state)
      nodevalue.arrayCandidate++=nodeNeighbor.arrayCandidate
      nodevalue
    })
    val candidateGraph=Graph(candidateNerVertice,resultGraph.edges)
    val random=new Random()
    val lastresult=candidateGraph.mapVertices((id,vt)=>{
      var candidate=""//结果String
      var candidateArray=ArrayBuffer[Any]()//扰动后节点，防止多次扰动
      vt.neighborArray.keys.foreach(key=>{
        if(key!=id.toInt) {
          if (random.nextInt(100) > p) {
            //不扰动
            candidate += key + " " + id + "   "
          }
          else {
            vt.arrayCandidate --= candidateArray
            val removeIdAttr = vt.arrayCandidate -- vt.neighborArray(id.toInt)
            removeIdAttr--=Array(id.toInt)
            val removeKeyAttr = vt.arrayCandidate -- vt.neighborArray(key)
            removeKeyAttr--=Array(key)
            if (removeIdAttr.size > 0) {
              val candidateNode = removeIdAttr(random.nextInt(removeIdAttr.size))
              candidateArray += candidateNode
              candidate += candidateNode + " " + id + "   "
            }
            else if (removeKeyAttr.size > 0) {
              val candidateNode = removeKeyAttr(random.nextInt(removeKeyAttr.size))
              candidateArray += candidateNode
              candidate += key + " " + candidateNode + "   "
            }
            else {
              candidate += key + " " + id + "   "
            }
          }
        }

      })
      candidate
    })
    lastresult.vertices.map(_._2)



  }



}
