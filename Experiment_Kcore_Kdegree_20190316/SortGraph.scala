package Experiment_Kcore_Kdegree_20190316

import org.apache.spark.graphx.{Graph, VertexRDD}

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer
/*
Design By 白夜
Date：20190.36
节点的度与核数排序，接收节点RDD，图RDD
节点的度排序
           节点RDD
           节点的目标度RDD
 */
object SortGraph {



  def SortByDegree(degrees: VertexRDD[Int],k :Int) ={
    //val sortDeree=degrees.sortBy(_._2,false)
    var i =0
    var j=0
    var u=0
    var PC1=0
    var PC2=0
    var PC3=0
    var last_partition_index=0
    val array01=new Array[Int](degrees.count().toInt)

    val sortDegree=degrees.sortBy(_._2,false).collect().foreach(attr=> { //排序
      array01(i) = attr._2.toInt
      i = i + 1
    })
    while(j<array01.length){
      if (j < k ) {
        array01(j)=array01(0)
        j=j+1
      }
      else if ( j> (degrees.count().toInt - k)){
        array01(j)=array01(degrees.count().toInt-k)
        j=j+1
      }
      else{

        PC1 = array01(last_partition_index) - array01(j)
        PC2 = 0
        PC3 = 0
        for (a <- j+1 to j+k) {
          PC1=PC1+array01(j+1)-array01(a)
          PC2=PC2+array01(j)-array01(a-1)
        }
        if(PC2 < PC1){
          for (h <- 1 to  k- 1) {
            array01(last_partition_index + h) = array01(last_partition_index) //新加入组的成员全部变成last_partition_index的值
          }
          last_partition_index = j
          j=j+k
        }
        else {
          //合并到前一组
          println(last_partition_index - k)
          array01(j) = array01(last_partition_index) //将i值变成last_partition_index-k值
          j=j+1
        }


      }
    }
    val SortByDegreeResult=degrees.sortBy(_._2,false).collect().map(attr=>{
      u=u+1

      (attr._1,(attr._2,array01(u-1)))
    })
    SortByDegreeResult
    }







  //需要添加核数
  def SortByCore(kcoreGraph: Graph[Int, Int],d :Int) = {


    //val sortCore=kcoreGraph.vertices.sortBy(_._2,false)
    val sortCore=kcoreGraph.vertices.mapValues((id ,value) =>{
     (value,1)
    })
    val sortCoreValue=sortCore.values.reduceByKey(_+_)
    val coreseq=sortCoreValue.map(attr=>{

      if(attr._2.toInt<d){
       (attr._1->(d-attr._2))
                         }
      else
        (0->0)
    }).filter(_._1!=0)

    val mapToBuffer=coreseq.collect().sortBy(_._1).toBuffer

    val kcoreResultGraph=kcoreGraph.vertices.sortBy(_._2).collect().map(attr=>{
      if(mapToBuffer.size>0){//需要添加的核
        if(attr._2>=mapToBuffer(0)._1){//分裂节点的核数必须大于添加节点的核数
          val a=mapToBuffer(0)
          mapToBuffer(0)=(mapToBuffer(0)._1,mapToBuffer(0)._2-1)
          if(mapToBuffer(0)._2==0){
            mapToBuffer.remove(0)

          }
          (attr._1,(attr._2,a._1))
        }
        else//分裂节点的核数小于添加节点的核数，需要更高核数的节点
          {
            (attr._1,(attr._2,0))
          }
      }

      else{//没有需要添加的核数
        (attr._1,(attr._2,0))
      }
    })

      kcoreResultGraph



  }

}
