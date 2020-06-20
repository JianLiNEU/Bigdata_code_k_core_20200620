import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.graphx.GraphLoader

object AnonyGraphAddLToC{
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("Kcore_RPDP").setMaster("local[5]")
    val sc = new SparkContext(conf)
    //图的读取
    //val graph=GraphLoader.edgeListFile(sc, "D:\\文档\\数据\\社区发现\\karateDemo01.txt")
    val graph=GraphLoader.edgeListFile(sc, "D:\\文档\\研究生课题20170424\\论文\\第一篇\\实验结果\\jazz.net")

    val a=graph.edges.mapValues(a=>{a.srcId+" "+a.dstId})

      a.saveAsTextFile("D:\\文档\\研究生课题20170424\\论文\\第一篇\\实验结果\\jazz")

  }
}