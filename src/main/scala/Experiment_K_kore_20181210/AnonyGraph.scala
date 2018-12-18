package Experiment_K_kore_20181210

import org.apache.spark.graphx.{Graph, VertexId}
import org.apache.spark.rdd.RDD

import scala.collection.mutable.ArrayBuffer
import scala.util.Random
/*
匿名算法的实现
               节点的数据结构
               （id   （map(正向)  map（反向）））
               map正向：Array（目的节点ID  源节点ID  跳数  是否匿名  核数   有效度数）
               map反向：Array（目的节点ID  源节点ID  跳数  是否匿名  核数   有效度数）
               首先对每一个triplets进行遍历循环{
               if(这条边是不需要扰动的){ 将源节点+目的节点 加入到结果集中}
               else（这条边是需要扰动的）{判断这条边的类型
                                                         即源节点与目的节点的核数相比
               }
             }
 */

object AnonyGraph {
  def AnonymityGraph(newGraph: Graph[(Map[Int, Array[Int]], Map[Int, Array[Int]]), Int]) = {
    val random=new Random()
    val ResultArray=new ArrayBuffer[String]()
    var candidateArray=ArrayBuffer[Int]()
    newGraph.triplets.foreach(triplet=>{

      candidateArray=null
      //判断不需要匿名的条件,通过边属性进行若为1，则扰动
      if(!(random.nextInt(9) > 0)){
        ResultArray+=triplet.srcId.toString+","+triplet.dstId.toString
      }
      else{
        //高核连接低核,即本地点的核数小于源节点的核数
        if(triplet.dstAttr._1(triplet.dstId.toInt)(4)<triplet.dstAttr._1(triplet.srcId.toInt)(4)){
          println("高核连接低核"+triplet.srcId+triplet.dstId)
          //高核连接低核，在高核的反向邻居选择,候选节点的选择为高核或同核
          triplet.srcAttr._2.keys.foreach(candidate=>{
            //选择候选节点,节点大于等于源节点（高核节点）
            if(triplet.srcAttr._2(candidate)(1)!=triplet.srcId.toInt&&triplet.srcAttr._2(candidate)(4)>=triplet.srcAttr._2(triplet.srcId)(4)){
              candidateArray+=triplet.srcAttr._2(candidate)(1)
            }

          })
          //对于反向传播节点个数不够，不考虑可达性，在正向传播节点选择
          if(candidateArray.size==0){
            triplet.srcAttr._1.keys.foreach(candidate=>{
            if(triplet.srcAttr._1(candidate)(1)!=triplet.srcId.toInt&&triplet.srcAttr._1(candidate)(4)>=triplet.srcAttr._1(triplet.srcId)(4)){
              candidateArray+=triplet.srcAttr._1(candidate)(1)
            }
            })
          }
          //将结果放入Array数组
          if (candidateArray.size!=0) {
            ResultArray += (candidateArray(random.nextInt(candidateArray.size - 1)).toString + "," + triplet.dstId)
          }
          else {
            ResultArray+=triplet.srcId.toString+","+triplet.dstId.toString
          }


        }


        //低核连接高核,即本节点的核数大于源节点的核数
        else if(triplet.dstAttr._1(triplet.dstId.toInt)(4)>triplet.dstAttr._1(triplet.srcId.toInt)(4)){
          println("低核连接高核"+triplet.srcId+triplet.dstId)
          //选择正向传播,本节点标签与高核的同核与高核

          triplet.dstAttr._1.keys.foreach(candidate=>{
            //选择候选节点,节点大于等于源节点（高核节点）
            if(triplet.dstAttr._1(candidate)(1)!=triplet.dstId.toInt&&triplet.dstAttr._1(candidate)(4)>=triplet.dstAttr._1(triplet.srcId)(4)){
              candidateArray+=triplet.dstAttr._1(candidate)(1)
            }

          })
          //对于反向传播节点个数不够，不考虑可达性，在正向传播节点选择
          if(candidateArray.size==0){
            triplet.dstAttr._2.keys.foreach(candidate=>{
              if(triplet.dstAttr._2(candidate)(1)!=triplet.srcId.toInt&&triplet.dstAttr._2(candidate)(4)>=triplet.dstAttr._2(triplet.srcId)(4)){
                candidateArray+=triplet.dstAttr._2(candidate)(1)
              }
            })
          }
          //将结果放入Array数组
          if (candidateArray.size!=0) {
            ResultArray += (triplet.srcId.toString+","+candidateArray(random.nextInt(candidateArray.size - 1)).toString )
          }
          else {
            ResultArray+=triplet.srcId.toString+","+triplet.dstId.toString
          }





        }
        //同核c型节点相连，源节点与目的节点的核数相同，源节点的核数与有效度数相等，目的节点的核数与有效度数相同
        else if(triplet.dstAttr._1(triplet.dstId.toInt)(4)==triplet.dstAttr._1(triplet.srcId.toInt)(4)&&triplet.dstAttr._1(triplet.dstId.toInt)(4)==triplet.dstAttr._1(triplet.dstId.toInt)(5)&&triplet.dstAttr._1(triplet.srcId.toInt)(4)==triplet.dstAttr._1(triplet.srcId.toInt)(5)){
          println("同核c型节点相连"+triplet.srcId+triplet.dstId)

        }
        //同核节点相连，存在节点的类型为L型
        else{
          println("同核L型节点相连"+triplet.srcId+triplet.dstId)

        }

      }

    })
  }

  /*def AnonymityGraph(TworesultGraph: RDD[(VertexId, (Map[Int, Array[Int]], Map[Int, Array[Int]]))]) = {

    val ResultArray=new ArrayBuffer[String]()//最终结果集合Array["1,2","3,4"]
    TworesultGraph.foreach(node=>{
      val candidate=" "
      node._2._1.keys.foreach(DirectAttr=>{

        if(node._2._1(DirectAttr)(3)==0&&node._2._1(DirectAttr)(2)==1){                    //不需要进行匿名
          ResultArray+=(node._2._1(DirectAttr)(0).toString+","+node._2._1(DirectAttr)(1).toString)

        }
        else if(node._2._1(DirectAttr)(2)==1)//需要进行匿名，并寻找出匿名结果放入ResultArray,本节点的核数与有效度数node._2._1(node._2._1(DirectAttr)(0))(4)(5)
        {
          //高核连接低核,即本地点的核数小于源节点的核数
          if(node._2._1(DirectAttr)(4)>node._2._1(node._2._1(DirectAttr)(0))(4)){
            println("高核连接低核"+node._2._1(DirectAttr)(0)+node._2._1(DirectAttr)(1))
          }
          //低核连接高核,即本节点的核数大于源节点的核数
          else if (node._2._1(DirectAttr)(4)<node._2._1(node._2._1(DirectAttr)(0))(4)){
            //选择正向传播,本节点标签与高核的同核与高核
            println("低核连接高核"+node._2._1(DirectAttr)(0)+node._2._1(DirectAttr)(1))

            node._2._1.keys.foreach(candidate=>{
              println("11111111111111")
              //选择不是目的节点自己，节点核数为同核或高核
              if(node._2._1(candidate)(1)!=node._2._1(candidate)(0)&&node._2._1(candidate)(4)>=node._2._1(node._2._1(DirectAttr)(0))(4)){
                println("候选节点为"+node._2._1(candidate)(1))

              }
            })




          }
          //同核c型节点相连，源节点与目的节点的核数相同，源节点的核数与有效度数相等，目的节点的核数与有效度数相同
          else if(node._2._1(DirectAttr)(4)==node._2._1(node._2._1(DirectAttr)(0))(4)&&node._2._1(DirectAttr)(4)==node._2._1(DirectAttr)(5)&&node._2._1(node._2._1(DirectAttr)(0))(4)==node._2._1(node._2._1(DirectAttr)(0))(5)){
            println("同核"+node._2._1(DirectAttr)(0)+node._2._1(DirectAttr)(1))
          }
          //同核节点相连，存在节点的类型为L型
          else{
            println("同核L型"+node._2._1(DirectAttr)(0)+node._2._1(DirectAttr)(1))

          }

        }
      }

      )

    }

    )

  }*/

}
