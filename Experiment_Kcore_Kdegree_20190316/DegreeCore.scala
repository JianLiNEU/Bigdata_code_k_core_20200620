package Experiment_Kcore_Kdegree_20190316
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.graphx._

object DegreeCore {

    def vprog(VD:VertexId, Value:Map[Int, Array[Int]], Message:Map[Int, Array[Int]]):Map[Int, Array[Int]] ={
      var newvalue=Value
      if(Message.exists(_._1==0)||Message.size==0){
        return(Value)
      }
      else{
        Message.foreach(v=>{
          if(!Value.contains(v._1)){
            newvalue+=(v._2(1)->Array(VD.toInt,v._2(1),v._2(2),v._2(3)))
          }
        }
        )
        newvalue
      }
    }
    def sendMsg( ET:EdgeTriplet[Map[Int, Array[Int]], Int]) :Iterator[(VertexId, Map[Int, Array[Int]])] ={
      Iterator((ET.srcId,ET.dstAttr),(ET.dstId,ET.srcAttr))
    }
    def mergeMsg (A1: Map[Int, Array[Int]], A2: Map[Int, Array[Int]]) : Map[Int, Array[Int]] = {
      A1.++(A2)
    }
  def main(args: Array[String]): Unit = {
      //spark基本设置
      val conf = new SparkConf().setAppName("APIN")
      val sc = new SparkContext(conf)
      val d=5
      //图的读取
      // val graph=GraphLoader.edgeListFile(sc, "D:\\文档\\数据\\社区发现\\karateDemo01.txt")
       val graph=GraphLoader.edgeListFile(sc, args(0))
     // val graph=GraphLoader.edgeListFile(sc, args(0))
      //GraphLoader.edgeListFile(sc,"/user/input/email")
      //val graph= GraphLoader.edgeListFile(sc,args(0),true,25,StorageLevel.MEMORY_AND_DISK,StorageLevel.MEMORY_AND_DISK)
    //计算社会网络节点核数
      val kcore =Kcore.KcoreGraph(graph)
      val kcoreGraph=graph.outerJoinVertices(kcore.vertices)((id, oldattr, newattr) =>newattr.getOrElse(0)).mapVertices((id, attr) =>(attr))
      val CoreDegreeGraph=kcoreGraph.vertices.leftJoin(graph.degrees)((id, oldattr, newattr) =>(oldattr,newattr.getOrElse(0)))
    //计算缺少目标核数

      val CoreVertice=SortGraph.SortByCore(kcoreGraph,d)
    //计算节点目标度
      //val SortByDegreeResult=SortGraph.SortByDegree(graph.degrees,k)
    /*SortByDegreeResult.foreach(attr=>{
      print(attr._2._1)
      print(attr._2._2)
      println()
    })*/
      //节点标签初始化,节点的标签为（id，本节点的核数，分裂节点的核数）
    val initGraph=Graph(sc.parallelize(CoreVertice),graph.edges).mapVertices((id,attr)=>{
        Map(id.toInt->Array(id.toInt,id.toInt,attr._1,attr._2))
      })
    //Pregel模型进行节点信息传播
     val PregelGraph=initGraph.pregel(Map(0->Array(0,0,0,0)),1,EdgeDirection.Either)(vprog,sendMsg,mergeMsg)
     var b=1
    val anonyResult=PregelGraph.mapVertices((id,attr)=>{
      if(attr(id.toInt)(3)>0){
        b=1
        var result=(id.toInt+100000).toString+" "+id.toString+"   "
        if(attr(id.toInt)(3)-1>0){

          attr.keys.foreach(a=>{
            if(attr(id.toInt)(3)-b>0&&attr(a)(2)>=attr(id.toInt)(2)&&attr(a)(1)!=id.toInt){
              b=b+1
              result+=(id.toInt+100000).toString+" "+attr(a)(1).toString+"   "
            }
          })
        }
        result
      }
      else
      {
        0.toString
      }
    })
    anonyResult.vertices.values.filter(_.!=(0.toString)).coalesce(1,true).saveAsTextFile(args(1))

  }


}
