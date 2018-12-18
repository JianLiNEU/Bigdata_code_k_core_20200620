package Experiment_K_kore_20181210

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.graphx._
import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel

import scala.collection.immutable.IntMap
import scala.util.Random
/*
main,
 */

object RPDPPregel {
  //反向传播节点标签
  def oppvprog(VD:VertexId, Value:Map[Int, Array[Int]], Message:Map[Int, Array[Int]]):Map[Int, Array[Int]] ={
    var newvalue=Value
    if(Message.exists(_._1==0)||Message.size==0){
      return(Value)
    }
    else{
      Message.foreach(v=>{
        if(!Value.contains(v._1)){
         // newvalue+=(v._2(1)->Array(v._2(1),VD.toInt,v._2(2)+1,v._2(3),v._2(4),v._2(5)))
          newvalue+=(v._2(1)->Array(VD.toInt,v._2(1),v._2(2)+1,v._2(3),v._2(4),v._2(5)))
        }
      }
      )
      newvalue
    }
  }


  //正向传播节点标签
  def vprog(VD:VertexId, Value:Map[Int, Array[Int]], Message:Map[Int, Array[Int]]):Map[Int, Array[Int]] ={
    var newvalue=Value
    if(Message.exists(_._1==0)||Message.size==0){
      return(Value)
    }
    else{
      Message.foreach(v=>{
        if(!Value.contains(v._1)){
          newvalue+=(v._2(1)->Array(VD.toInt,v._2(1),v._2(2)+1,v._2(3),v._2(4),v._2(5)))
        }
      }
      )
      newvalue
    }


  }

  def sendMsg( ET:EdgeTriplet[Map[Int, Array[Int]], Int]) :Iterator[(VertexId, Map[Int, Array[Int]])] ={
    Iterator((ET.dstId,ET.srcAttr))

  }

  def mergeMsg (A1: Map[Int, Array[Int]], A2: Map[Int, Array[Int]]) : Map[Int, Array[Int]] = {
    A1.++(A2)
  }




  def main(args: Array[String]): Unit = {
    //spark基本设置
    val conf = new SparkConf().setAppName("Kcore_RPDP").setMaster("local[2]")
    val sc = new SparkContext(conf)
    //图的读取
    val graph=GraphLoader.edgeListFile(sc, "D:\\文档\\数据\\社区发现\\karateDemo01.txt")
    //GraphLoader.edgeListFile(sc,"/user/input/email")
    //val graph= GraphLoader.edgeListFile(sc,args(0),true,10,StorageLevel.MEMORY_AND_DISK,StorageLevel.MEMORY_AND_DISK).partitionBy(PartitionStrategy.RandomVertexCut)

    val kcore =Kcore.KcoreGraph(graph)
    val kcoreGraph=graph.outerJoinVertices(kcore.vertices)((id, oldattr, newattr) =>newattr.getOrElse(0)).mapVertices((id, attr) =>(attr))
    val effectGraph=EffectGrdee.EffectGraph(kcoreGraph)
    val random=new Random()
    val initGraph=effectGraph.mapVertices((id,attr)=> {

        (Map(id.toInt -> Array(id.toInt, id.toInt, 0, 0,attr._1,attr._2)))
    })
 /*   val initGraph=effectGraph.mapVertices((id,attr)=> {
      if (random.nextInt(9) > 0)
        (Map(id.toInt -> Array(id.toInt, id.toInt, 0, 1,attr._1,attr._2)))
      else
        (Map(id.toInt -> Array(id.toInt, id.toInt, 0, 0,attr._1,attr._2)))
    })
*/



    // val initalGraph= graph.reverse.outerJoinVertices(graph.reverse.outDegrees){(id,attr,outg)=>outg}
    //正向传播pregel
    val rpdpGraph=initGraph.pregel(Map(0->Array(0,0,0,0)),3,EdgeDirection.Either)(vprog,sendMsg,mergeMsg)
    //反向传播pregel
    val opposedGraph=initGraph.reverse.pregel(Map(0->Array(0,0,0,0)),3,EdgeDirection.Either)(oppvprog,sendMsg,mergeMsg)

    //将两个图的结果合并，合并后卫节点RDD
    val TworesultGraph=rpdpGraph.vertices.join(opposedGraph.vertices)
    //进行随机边赋值，将结果为边的属性


    //rpdpGraph.edges.saveAsTextFile("D:\\文档\\数据\\社区发现\\RPDP")
    //rpdpGraph.vertices.saveAsTextFile("user/output/RPDP01")
    //遍历结果
    //rpdpGraph.vertices.foreach(u=>u._2._2.foreach(u1=>println("节点id"+u._1+"节点属性"+u1.toString())))
    val newGraph=Graph(TworesultGraph,graph.edges)
    val AnonyGraphResult=AnonyGraph.AnonymityGraph(newGraph)
    TworesultGraph.foreach(f=> {
      println("正向传播节点属性")
      f._2._1.keys.foreach(v =>{
        f._2._1(v).foreach(

          print(_)
        )
        println()
        println("---------------")
      })
      println("反向传播节点属性" )
      f._2._2.keys.foreach(v =>{
        f._2._2(v).foreach(

          print(_)
        )
        println()
        println("---------------")
      })
    }
    )













  }

}
