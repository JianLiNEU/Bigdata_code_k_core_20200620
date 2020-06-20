package Experiment_Result

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.graphx.{Graph, GraphLoader}

import scala.reflect.ClassTag

object Global_clustering_coefficient {
  def main(args: Array[String]): Unit = {
    //spark基本设置
    val conf = new SparkConf().setAppName("Kcore_RPDP").setMaster("local[5]")
    val sc = new SparkContext(conf)
    //图的读取
    val graph=GraphLoader.edgeListFile(sc, "D:\\文档\\研究生课题20170424\\论文\\第一篇\\实验结果\\34_2_0.5.txt")
    println(globalClusteringCoefficient(graph))
    println(localClusteringCoefficient(graph))


  }

    //全局聚类系数
  def globalClusteringCoefficient[VD: ClassTag, ED: ClassTag](g:Graph[VD, ED]) = {
    val numerator  = g.triangleCount().vertices.map(_._2).reduce(_ + _)
    val denominator = g.inDegrees.map{ case (_, d) => d*(d-1) / 2.0 }.reduce(_ + _)
    if(denominator == 0) 0.0 else numerator / denominator
  }
  //局部聚类系数
  def localClusteringCoefficient[VD: ClassTag, ED: ClassTag](g: Graph[VD, ED]) = {
    val triCountGraph = g.triangleCount()
    val maxTrisGraph = g.inDegrees.mapValues(srcAttr => srcAttr*(srcAttr-1) / 2.0 )
    triCountGraph.vertices.innerJoin(maxTrisGraph){ (vid, a, b) => if(b == 0) 0 else a / b }
  }
  //网络聚类系数
    //println(localClusteringCoefficient(testGraph).map(_._2).sum() / testGraph.vertices.count())



}
