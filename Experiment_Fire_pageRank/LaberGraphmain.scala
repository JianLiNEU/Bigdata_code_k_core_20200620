package Experiment_Fire_pageRank

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.graphx._

object LaberGraphmain {
  //pregel三个函数
  def vprog(vid: VertexId, attr: VertexState, message: VertexState): VertexState = {
    val nodeattr = attr
    if (nodeattr.nodeFireL < message.nodeFireL) {
      nodeattr.community = message.community
      nodeattr.changed = true
      nodeattr
    }
    else {
      nodeattr
    }

  }

  def sendMsg(ET: EdgeTriplet[VertexState, Double]): Iterator[(VertexId, VertexState)] = {
    Iterator((ET.dstId, ET.srcAttr), (ET.srcId, ET.dstAttr))
  }

  def mergeMsg(v1: VertexState, v2: VertexState): VertexState = {
    if (v1.nodeFireL > v2.nodeFireL) {
      v1
    }
    else (
      v2
      )
  }

  def NewGraph(compressNewGraph: Graph[VertexState, Double]) = {
    //计算新节点的Fire值
    val interalNodeFire = compressNewGraph.vertices.values.map(vdata => (vdata.community, vdata.nodeFireL)).reduceByKey(_ + _)
    //创建新图的新节点
    val newVerts = interalNodeFire.map(attr => {
      val state = new VertexState()
      state.community = attr._1
      state.changed = false
      state.nodeFireL = attr._2
      (attr._1, state)
    }).cache()
    val newGraph = Graph(compressNewGraph.vertices, compressNewGraph.edges)
    val newEdge = newGraph.triplets.flatMap(et => {
      val src = math.min(et.srcAttr.community, et.dstAttr.community)
      val dst = math.max(et.srcAttr.community, et.dstAttr.community)
      if (src != dst) {
        Iterator(new Edge(src, dst, et.attr))
      }
      else Iterator.empty
    }).cache()

    //创建新图的新边
    val compressedGraph = Graph(newVerts,newEdge).partitionBy(PartitionStrategy.EdgePartition2D).groupEdges(_+_)
    compressedGraph
   }

  def compressGraph(sc: SparkContext, kcoreGraph: Graph[VertexState, Double]):Graph[VertexState, Double] = {
    val state=new VertexState()
    val pregelcompressGraph=kcoreGraph.pregel(state,1,EdgeDirection.Either)(vprog,sendMsg,mergeMsg)
    pregelcompressGraph
  }

  def main(args: Array[String]): Unit = {
      val conf = new SparkConf().setAppName("Firely_degree")
      val sc = new SparkContext(conf)

      //图的读取
      val originGraph = GraphLoader.edgeListFile(sc, args(0))
      //val originGraph=GraphLoader.edgeListFile(sc, args(0))
      val p = 49
      val pagerankGraph = originGraph.pageRank(0.00001)
      //初始化节点状态
      val initGraph = pagerankGraph.mapVertices((id, value) => {
        val state = new VertexState()
        state.community = id
        state.changed = false
        state.nodeFireL = value.toFloat
        state
      })

      val compressPregelGraph = compressGraph(sc, initGraph)
      //将社区值map到原始图,这是1邻居压缩结果图
      val resultGraph01 = initGraph.outerJoinVertices(compressPregelGraph.vertices)((id, attr, newattr) => {
        val state = new VertexState()
        (id, newattr.getOrElse(state))
      })
      //########################压缩第一次扰动
     /* val result = graphAnonymity.anonymity(resultGraph01, sc, p)
      result.coalesce(1, true).saveAsTextFile(args(1))*/



    //为了第二次压缩，创建新图，以社区值为新节点，将同一社区的节点Fire值相加作为节点的Firel值。
    val compressNewGraph02=NewGraph(compressPregelGraph)
    //第二次压缩
    val compressPregelGraph02=compressGraph(sc,compressNewGraph02)

    //将结果值map到原始图，这是2邻居压缩结果图
    val resultGraph02=resultGraph01.vertices.map(attr=>(attr._2._2.community,attr._1)).join(compressNewGraph02.vertices).map(attr=>{
      (attr._2._1,attr._2._2)
    })
    //####################压缩第二次扰动
    val resultGraph=Graph(resultGraph02,originGraph.edges)
   /* val result=graphAnonymity.anonymity0203(resultGraph,sc,p)
    result.coalesce(1,true).saveAsTextFile(args(1))*/



      //第三次压缩
    val compressNewGraph03=NewGraph(compressPregelGraph02)
    val compressPregelGraph03=compressGraph(sc,compressNewGraph03)
    val resultGraph03=resultGraph02.map(attr=>{(attr._2.community,attr._1)}).join(compressPregelGraph03.vertices).map(attr1=>(attr1._2._1,attr1._2._2))
    //将结果map到原始图中

    //#####################压缩第三次扰动
    val newresultGraph03=Graph(resultGraph02,originGraph.edges)
    val result03=graphAnonymity.anonymity0203(newresultGraph03,sc,p)
    result03.coalesce(1,true).saveAsTextFile(args(1))

  }






}