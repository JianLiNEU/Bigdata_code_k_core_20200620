package Experiment_Kcore_20190422

import org.apache.spark.graphx.Graph

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
  def AnonymityGraph(rpdpGraph: Graph[VertexState, Int]) = {


    val random = new Random()
    var candidateArray = ArrayBuffer[Int]()
    val resultLastGraph = rpdpGraph.mapTriplets(triplet => {
      candidateArray.clear()
      if (!(random.nextInt(100) > 50)) {
        println("不进行扰动" + triplet.srcId.toString + "," + triplet.dstId.toString)
        //triplet.srcAttr._3+=triplet.srcId.toString+" "+triplet.dstId.toString
        triplet.srcId.toString + " " + triplet.dstId.toString
        //ResultArray+=triplet.srcId.toString+" "+triplet.dstId.toString
      }
      else {
        if (triplet.dstAttr.RNTE(triplet.dstId.toInt)(4) < triplet.dstAttr.RNTE(triplet.srcId.toInt)(4)) {
          val lowerCore = triplet.dstAttr.RNTE(triplet.dstId.toInt)(4)

          triplet.srcAttr.RNTE.keys.foreach(candidate => {
            if (triplet.srcAttr.RNTE(candidate)(3) != triplet.dstId.toInt && triplet.srcAttr.RNTE(candidate)(1) != triplet.dstId.toInt && triplet.srcAttr.RNTE(candidate)(1) != triplet.srcId.toInt && triplet.srcAttr.RNTE(candidate)(4) >= lowerCore) {
              candidateArray += candidate
            }
          })
          //对于反向传播节点个数不够，不考虑可达性，在正向传播节点选择

          if (candidateArray.size != 0) {
            println("高核连接低核" + triplet.srcId.toString + "," + triplet.dstId.toString)
            println(candidateArray(random.nextInt(candidateArray.size)).toString + " " + triplet.dstId)
            (candidateArray(random.nextInt(candidateArray.size)).toString + " " + triplet.dstId)
          }
          else {
            (triplet.srcId.toString + " " + triplet.dstId.toString)
          }


        }


        //低核连接高核,即本节点的核数大于源节点的核数
        //选择正向传播,本节点标签与高核的同核与高核
        //选择候选节点,节点大于等于源节点（高核节点）
        else if (triplet.dstAttr.RNTE(triplet.dstId.toInt)(4) > triplet.dstAttr.RNTE(triplet.srcId.toInt)(4)) {
          println("低核连接高核" + triplet.srcId + triplet.dstId)
          triplet.dstAttr.RNTE.keys.foreach(candidate => {
            if (triplet.dstAttr.RNTE(candidate)(3) != triplet.srcId.toInt && triplet.dstAttr.RNTE(candidate)(1) != triplet.dstId.toInt && triplet.dstAttr.RNTE(candidate)(4) >= triplet.dstAttr.RNTE(triplet.srcId.toInt)(4) && triplet.dstAttr.RNTE(candidate)(1) != triplet.srcId.toInt) {
              candidateArray += triplet.dstAttr.RNTE(candidate)(1)
            }
          })
          if (candidateArray.size != 0) {
            (triplet.srcId.toString + " " + candidateArray(random.nextInt(candidateArray.size)).toString)
          }
          else {
            println("低核连接高核" + triplet.srcId + triplet.dstId)
            println(triplet.srcId.toString + " " + triplet.dstId.toString)
            (triplet.srcId.toString + " " + triplet.dstId.toString)
          }
        }




        //同核c型节点相连，源节点与目的节点的核数相同，源节点的核数与有效度数相等，目的节点的核数与有效度数相同
        //同核C型之间扰动，则在源节点与目的节点进行两次扰动。
        //扰动源节点，则选择源节点的反向传播标签列表中节点，将与源节点的反向连接节点与目的节点相连；
        //扰动目的节点，则选择目的节点的正向传播标签列表中节点，将源节点与目的节点正向传递节点相连。
        //选择方向保证节点的可达性。
        //选择目的节点正向候选节点属性,节点大于等于源节点（高核节点）, 考虑到同核，目的节点的候选节点不能包含源节点
        else if (triplet.dstAttr.RNTE(triplet.dstId.toInt)(4) == triplet.dstAttr.RNTE(triplet.srcId.toInt)(4) && triplet.dstAttr.RNTE(triplet.dstId.toInt)(4) == triplet.dstAttr.RNTE(triplet.dstId.toInt)(5) && triplet.dstAttr.RNTE(triplet.srcId.toInt)(4) == triplet.dstAttr.RNTE(triplet.srcId.toInt)(5)) {
         var a=" "
          println("同核c型节点相连" + triplet.srcId + triplet.dstId)
          triplet.dstAttr.RNTE.keys.foreach(candidate => {
            if (triplet.dstAttr.RNTE(candidate)(3) != triplet.srcId.toInt && triplet.dstAttr.RNTE(candidate)(1) != triplet.dstId.toInt && triplet.dstAttr.RNTE(candidate)(4) >= triplet.dstAttr.RNTE(triplet.dstId.toInt)(4) && triplet.dstAttr.RNTE(candidate)(1) != triplet.srcId.toInt) {
              candidateArray += triplet.dstAttr.RNTE(candidate)(1)
            }
          })
          if (candidateArray.size != 0) {
            println("同核c型节点相连" + triplet.srcId + triplet.dstId)
            println(triplet.srcId.toString + " " + candidateArray(random.nextInt(candidateArray.size)).toString)
           a= (triplet.srcId.toString + " " + candidateArray(random.nextInt(candidateArray.size)).toString)
          }
          else {
            println("同核c型节点相连" + triplet.srcId + triplet.dstId)
            println(triplet.srcId.toString + " " + triplet.dstId.toString)
            a=(triplet.srcId.toString + " " + triplet.dstId.toString)
          }

          // candidateArray=null//保证节点核数不变，进行两次扰动
          candidateArray.clear()

          //选择目的节点反向候选节点,节点大于等于源节点（高核节点）
          //选择正向节点
          triplet.srcAttr.RNTE.keys.foreach(candidate => {
            if (triplet.srcAttr.RNTE(candidate)(3) != triplet.dstId.toInt && triplet.srcAttr.RNTE(candidate)(1) != triplet.srcId.toInt && triplet.srcAttr.RNTE(candidate)(4) >= triplet.srcAttr.RNTE(triplet.srcId.toInt)(4) && triplet.srcAttr.RNTE(candidate)(1) != triplet.dstId.toInt) {
              candidateArray += triplet.srcAttr.RNTE(candidate)(1)
            }
          })

          if (candidateArray.size != 0) {
            a+"  "+(candidateArray(random.nextInt(candidateArray.size)).toString + " " + triplet.dstId.toString)
          }
          else {
            println("同核c型节点相连" + triplet.srcId + triplet.dstId)
            println(triplet.srcId.toString + " " + triplet.dstId.toString)
            a+"  "+(triplet.srcId.toString + " " + triplet.dstId.toString)

          }

        }



        //同核节点相连，存在节点的类型为L型
        //同核节点,挑选出谁为L型
        //L型节点与L型节点相连,先选择正向传播的节点属性
        //选择目的节点正向候选节点属性,节点大于等于源节点（高核节点）, 考虑到同核，目的节点的候选节点不能包含源节点
        else {
          //dst节点有效度数大于核数
          if (triplet.dstAttr.RNTE(triplet.dstId.toInt)(5) > triplet.dstAttr.RNTE(triplet.dstId.toInt)(4) && triplet.dstAttr.RNTE(triplet.srcId.toInt)(5) > triplet.dstAttr.RNTE(triplet.srcId.toInt)(4)) {
            triplet.dstAttr.RNTE.keys.foreach(candidate => {
              if (triplet.dstAttr.RNTE(candidate)(3) != triplet.srcId.toInt && triplet.dstAttr.RNTE(candidate)(1) != triplet.dstId.toInt && triplet.dstAttr.RNTE(candidate)(4) >= triplet.dstAttr.RNTE(triplet.dstId.toInt)(4) && triplet.dstAttr.RNTE(candidate)(1) != triplet.srcId.toInt) {
                candidateArray += triplet.dstAttr.RNTE(candidate)(1)
              }
            })
            if (candidateArray.size == 0)
            {
              //判断是否为L-L型节点相连
                triplet.srcAttr.RNTE.keys.foreach(candidate => {
                //src节点有效度数大于核数
                if (triplet.srcAttr.RNTE(candidate)(3) != triplet.dstId.toInt && triplet.srcAttr.RNTE(candidate)(1) != triplet.srcId.toInt && triplet.srcAttr.RNTE(candidate)(5) >= triplet.srcAttr.RNTE(triplet.srcId.toInt)(4) && triplet.srcAttr.RNTE(candidate)(1) != triplet.dstId.toInt) {
                  candidateArray += triplet.srcAttr.RNTE(candidate)(1)
                }
                 })
               if (candidateArray.size == 0){
                 (triplet.srcId.toString + " " + triplet.dstId.toString)
               }
              else{
                 triplet.srcAttr.RNTE(triplet.srcId.toInt)(5) -= 1
                 println("同核L型节点相连" + triplet.srcId + triplet.dstId)
                 (candidateArray(random.nextInt(candidateArray.size)).toString + " " + triplet.dstId.toString)
               }
            }
            else
            {
                  triplet.dstAttr.RNTE(triplet.srcId.toInt)(5) -= 1
                  (triplet.srcId.toString+ " " +candidateArray(random.nextInt(candidateArray.size)).toString )
            }
          }
          else {
              triplet.srcAttr.RNTE.keys.foreach(candidate => {
                //src节点有效度数大于核数
                if (triplet.srcAttr.RNTE(candidate)(3) != triplet.dstId.toInt && triplet.srcAttr.RNTE(candidate)(1) != triplet.srcId.toInt && triplet.srcAttr.RNTE(candidate)(5) >= triplet.srcAttr.RNTE(triplet.srcId.toInt)(4) && triplet.srcAttr.RNTE(candidate)(1) != triplet.dstId.toInt) {
                  candidateArray += triplet.srcAttr.RNTE(candidate)(1)
                }
              })
              if (candidateArray.size == 0) {
               (triplet.srcId.toString + " " + triplet.dstId.toString)
              }
             else
              {
                triplet.srcAttr.RNTE(triplet.srcId.toInt)(5) -= 1
                println("同核L型节点相连" + triplet.srcId + triplet.dstId)
                (candidateArray(random.nextInt(candidateArray.size)).toString + " " + triplet.dstId.toString)

              }

          }





        } //同核L型

      }

    })
    resultLastGraph


  }
}
