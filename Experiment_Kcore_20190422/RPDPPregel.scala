package Experiment_Kcore_20190422

import Experiment_K_kore_20181210.{AnonyGraph, EffectGrdee}
import Experiment_test.Kcore
import org.apache.spark.graphx._
import org.apache.spark.{SparkConf, SparkContext}

import scala.util.Random

object RPDPPregel {

 def vprog(Vid:VertexId, attr:VertexState, vs:Map[Int, Array[Int]]): VertexState = {
    if(vs.exists(_._1==0)||vs.size==0){
      return attr
    }
    else {
      var state=new VertexState
       state.RNTE++=attr.RNTE
      vs.foreach(v=>{
        if(!attr.RNTE.contains(v._1)){
          state.RNTE+=(v._2(1)->Array(Vid.toInt,v._2(1),v._2(2)+1,v._2(0),v._2(4),v._2(5)))

        }
      })
       return state
    }
  }

  def sendMsg(ET:EdgeTriplet[VertexState, Int] ): Iterator[(VertexId, Map[Int, Array[Int]])] = {
    Iterator((ET.dstId,ET.srcAttr.RNTE),(ET.srcId,ET.dstAttr.RNTE))
  }

  def mergeMsg(A1:Map[Int, Array[Int]], A2:Map[Int, Array[Int]]) : Map[Int, Array[Int]] ={
    A1++(A2)
  }



  def main(args: Array[String]): Unit = {
    //spark基本设置
    val conf = new SparkConf().setAppName("Kcore")
    val sc = new SparkContext(conf)
    //图的读取
   //val graph=GraphLoader.edgeListFile(sc, "D:\\文档\\数据\\社区发现\\karateDemo01.txt")
   val graph=GraphLoader.edgeListFile(sc, args(0))
    val kcore =Kcore.KcoreGraph(graph)
    val kcoreGraph=graph.outerJoinVertices(kcore.vertices)((id, oldattr, newattr) =>newattr.getOrElse(0)).mapVertices((id, attr) =>(attr))
    val effectGraph=EffectGrdee.EffectGraph(kcoreGraph)
    val initGraph=effectGraph.mapVertices((id,attr)=> {
      val state=new VertexState
      state.RNTE=Map(id.toInt -> Array(id.toInt, id.toInt, 0,0,attr._1,attr._2))
      state
    })
    val rpdpGraph=initGraph.pregel(Map(0->Array(0,0,0,0,0,0)),4,EdgeDirection.Either)(vprog,sendMsg,mergeMsg)
    val AnonyGraphResult=AnonyGraph.AnonymityGraph(rpdpGraph)
     AnonyGraphResult.triplets.map(et=>et.attr).coalesce(1,true)saveAsTextFile(args(1))

  }

 /*//正向传播节点标签
 def vprog(VD:VertexId, Value:Map[Int, Array[Int]], Message:Map[Int, Array[Int]]):Map[Int, Array[Int]] ={
   var newvalue=Value
   if(Message.exists(_._1==0)||Message.size==0){
     return(Value)
   }
   else{
     Message.foreach(v=>{
       if(!Value.contains(v._1)){
         newvalue+=(v._2(1)->Array(VD.toInt,v._2(1),v._2(2)+1,v._2(0),v._2(4),v._2(5)))
       }
     }
     )
     newvalue
   }
 }

  def sendMsg( ET:EdgeTriplet[Map[Int, Array[Int]], Int]) :Iterator[(VertexId, Map[Int, Array[Int]])] ={
    Iterator((ET.dstId,ET.srcAttr),(ET.srcId,ET.dstAttr))

  }

  def mergeMsg (A1: Map[Int, Array[Int]], A2: Map[Int, Array[Int]]) : Map[Int, Array[Int]] = {
    A1.++(A2)
  }




  def main(args: Array[String]): Unit = {
    //spark基本设置
    val conf = new SparkConf().setAppName("Kcore_RPDP").setMaster("local[5]")
    val sc = new SparkContext(conf)
    //图的读取
    val graph=GraphLoader.edgeListFile(sc, "D:\\文档\\数据\\社区发现\\karateDemo01.txt")
    //  val graph=GraphLoader.edgeListFile(sc, args(0))
    //GraphLoader.edgeListFile(sc,"/user/input/email")
    //val graph= GraphLoader.edgeListFile(sc,args(0),true,25,StorageLevel.MEMORY_AND_DISK,StorageLevel.MEMORY_AND_DISK)


    val kcore =Kcore.KcoreGraph(graph)
    val kcoreGraph=graph.outerJoinVertices(kcore.vertices)((id, oldattr, newattr) =>newattr.getOrElse(0)).mapVertices((id, attr) =>(attr))
    val effectGraph=EffectGrdee.EffectGraph(kcoreGraph)
    val random=new Random()
    val initGraph=effectGraph.mapVertices((id,attr)=> {

      (Map(id.toInt -> Array(id.toInt, id.toInt, 0, 0,attr._1,attr._2)))
    })

    // val initalGraph= graph.reverse.outerJoinVertices(graph.reverse.outDegrees){(id,attr,outg)=>outg}
    //正向传播pregel
    val rpdpGraph=initGraph.pregel(Map(0->Array(0,0,0,0)),3,EdgeDirection.Either)(vprog,sendMsg,mergeMsg)

    //将两个图的结果合并，合并后卫节点RDD

    val AnonyGraphResult=AnonyGraph.AnonymityGraph(rpdpGraph)
    AnonyGraphResult.triplets.map(et=>et.attr).coalesce(1,true)saveAsTextFile(args(1))
    //输出结果
    //ResultRDD.saveAsTextFile("/user/output/Kcore02")
*/












}
