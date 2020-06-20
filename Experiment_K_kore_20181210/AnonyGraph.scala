package Experiment_K_kore_20181210

import org.apache.spark.{SparkConf, SparkContext}
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



  // var ResultArray=ArrayBuffer[String]()

  def AnonymityGraph(newGraph: Graph[(Map[Int, Array[Int]], Map[Int, Array[Int]]), Int]) = {
    val random=new Random()
    var candidateArray=ArrayBuffer[Int]()
    val resultLastGraph=newGraph.mapTriplets(triplet=>{
      candidateArray.clear()
      //candidateArray.drop(candidateArray.size)
      //使用随即函数进行判断是否进行扰动
      //               使用随即函数的比例控制边保护的条件
      if(!(random.nextInt(100) >75)){
        println("不进行扰动"+triplet.srcId.toString+","+triplet.dstId.toString)
        //triplet.srcAttr._3+=triplet.srcId.toString+" "+triplet.dstId.toString
      triplet.srcId.toString+" "+triplet.dstId.toString
        //ResultArray+=triplet.srcId.toString+" "+triplet.dstId.toString
      }
        /*
        将连接的边分为四类：
                           高核连接低核
                           低核连接高核
                           同核有L型
                           同核C型节点
         */
      //高核连接低核,即本地点的核数小于源节点的核数
      //高核连接低核，在高核的反向邻居选择,候选节点的选择为高核或同核
      //选择候选节点,节点大于等于源节点（高核节点）
      else{
        if(triplet.dstAttr._1(triplet.dstId.toInt)(4)<triplet.dstAttr._1(triplet.srcId.toInt)(4)){
          val lowerCore=triplet.dstAttr._1(triplet.dstId.toInt)(4)

          triplet.srcAttr._2.keys.foreach(candidate=>{
            if(triplet.srcAttr._2(candidate)(3)!=triplet.dstId.toInt&&triplet.srcAttr._2(candidate)(1)!=triplet.dstId.toInt&&triplet.srcAttr._2(candidate)(1)!=triplet.srcId.toInt&&triplet.srcAttr._2(candidate)(4)>=lowerCore){
              candidateArray+=candidate
            }
          })
          //对于反向传播节点个数不够，不考虑可达性，在正向传播节点选择
          if(candidateArray.size==0){
            triplet.srcAttr._1.keys.foreach(candidate=>{
            if(triplet.srcAttr._1(candidate)(3)!=triplet.dstId.toInt&&triplet.srcAttr._1(candidate)(1)!=triplet.srcId.toInt&&triplet.srcAttr._1(candidate)(4)>=lowerCore){
              candidateArray+=triplet.srcAttr._1(candidate)(1)
            }
            })
          }
          if (candidateArray.size!=0) {
            println("高核连接低核"+triplet.srcId.toString+","+triplet.dstId.toString)
            println(candidateArray(random.nextInt(candidateArray.size)).toString + " " + triplet.dstId)
            (candidateArray(random.nextInt(candidateArray.size)).toString + " " + triplet.dstId)
          }
          else {
            println("高核连接低核"+triplet.srcId.toString+","+triplet.dstId.toString)
            println(triplet.srcId.toString+" "+triplet.dstId.toString)
            (triplet.srcId.toString+" "+triplet.dstId.toString)
          }

        }


        //低核连接高核,即本节点的核数大于源节点的核数
        //选择正向传播,本节点标签与高核的同核与高核
        //选择候选节点,节点大于等于源节点（高核节点）
        else if(triplet.dstAttr._1(triplet.dstId.toInt)(4)>triplet.dstAttr._1(triplet.srcId.toInt)(4)){
          println("低核连接高核"+triplet.srcId+triplet.dstId)
          triplet.dstAttr._1.keys.foreach(candidate=>{
            if(triplet.dstAttr._1(candidate)(3)!=triplet.srcId.toInt&&triplet.dstAttr._1(candidate)(1)!=triplet.dstId.toInt&&triplet.dstAttr._1(candidate)(4)>=triplet.dstAttr._1(triplet.srcId.toInt)(4)&&triplet.dstAttr._1(candidate)(1)!=triplet.srcId.toInt){
              candidateArray+=triplet.dstAttr._1(candidate)(1)
            }
          })
          //对于反向传播节点个数不够，不考虑可达性，在正向传播节点选择
          if(candidateArray.size==0){
            triplet.dstAttr._2.keys.foreach(candidate=>{
              if(triplet.dstAttr._2(candidate)(3)!=triplet.srcId.toInt&&triplet.dstAttr._2(candidate)(1)!=triplet.srcId.toInt&&triplet.dstAttr._2(candidate)(4)>=triplet.dstAttr._1(triplet.srcId.toInt)(4)){
                candidateArray+=triplet.dstAttr._2(candidate)(1)
              }
            })
          }
          if (candidateArray.size!=0) {
            println("低核连接高核"+triplet.srcId+triplet.dstId)
            println(triplet.srcId.toString+" "+candidateArray(random.nextInt(candidateArray.size)).toString )
            (triplet.srcId.toString+" "+candidateArray(random.nextInt(candidateArray.size)).toString )
          }
          else {
            println("低核连接高核"+triplet.srcId+triplet.dstId)
            println(triplet.srcId.toString+" "+triplet.dstId.toString)
            (triplet.srcId.toString+" "+triplet.dstId.toString)
          }
        }




        //同核c型节点相连，源节点与目的节点的核数相同，源节点的核数与有效度数相等，目的节点的核数与有效度数相同
        //同核C型之间扰动，则在源节点与目的节点进行两次扰动。
        //扰动源节点，则选择源节点的反向传播标签列表中节点，将与源节点的反向连接节点与目的节点相连；
        //扰动目的节点，则选择目的节点的正向传播标签列表中节点，将源节点与目的节点正向传递节点相连。
        //选择方向保证节点的可达性。
        //选择目的节点正向候选节点属性,节点大于等于源节点（高核节点）, 考虑到同核，目的节点的候选节点不能包含源节点
        else if(triplet.dstAttr._1(triplet.dstId.toInt)(4)==triplet.dstAttr._1(triplet.srcId.toInt)(4)&&triplet.dstAttr._1(triplet.dstId.toInt)(4)==triplet.dstAttr._1(triplet.dstId.toInt)(5)&&triplet.dstAttr._1(triplet.srcId.toInt)(4)==triplet.dstAttr._1(triplet.srcId.toInt)(5)){
          println("同核c型节点相连"+triplet.srcId+triplet.dstId)
          triplet.dstAttr._1.keys.foreach(candidate=>{
            if(triplet.dstAttr._1(candidate)(3)!=triplet.srcId.toInt&&triplet.dstAttr._1(candidate)(1)!=triplet.dstId.toInt&&triplet.dstAttr._1(candidate)(4)>=triplet.dstAttr._2(triplet.dstId.toInt)(4)&&triplet.dstAttr._1(candidate)(1)!=triplet.srcId.toInt){
              candidateArray+=triplet.dstAttr._1(candidate)(1)
            }
          })
          //正向节点不够，选择反向连接的节点
          if(candidateArray.size==0){
            triplet.dstAttr._2.keys.foreach(candidate=>{
              if(triplet.dstAttr._2(candidate)(3)!=triplet.srcId.toInt&&triplet.dstAttr._2(candidate)(1)!=triplet.dstId.toInt&&triplet.dstAttr._2(candidate)(4)>=triplet.dstAttr._2(triplet.dstId.toInt)(4)&&triplet.dstAttr._2(candidate)(1)!=triplet.srcId.toInt){
                candidateArray+=triplet.dstAttr._2(candidate)(1)
              }
            })
          }
          if (candidateArray.size!=0) {
            println("同核c型节点相连"+triplet.srcId+triplet.dstId)
            println(triplet.srcId.toString+" "+candidateArray(random.nextInt(candidateArray.size)).toString )
            (triplet.srcId.toString+" "+candidateArray(random.nextInt(candidateArray.size)).toString )
          }
          else {
            println("同核c型节点相连"+triplet.srcId+triplet.dstId)
            println(triplet.srcId.toString+" "+triplet.dstId.toString)
            (triplet.srcId.toString+" "+triplet.dstId.toString)
          }

          // candidateArray=null//保证节点核数不变，进行两次扰动
          candidateArray.clear()

          //选择目的节点反向候选节点,节点大于等于源节点（高核节点）
          //选择正向节点
          triplet.srcAttr._2.keys.foreach(candidate=>{
            if(triplet.srcAttr._2(candidate)(3)!=triplet.dstId.toInt&&triplet.srcAttr._2(candidate)(1)!=triplet.srcId.toInt&&triplet.srcAttr._2(candidate)(4)>=triplet.srcAttr._2(triplet.srcId.toInt)(4)&&triplet.srcAttr._2(candidate)(1)!=triplet.dstId.toInt){
              candidateArray+=triplet.srcAttr._2(candidate)(1)
            }
          })
          if(candidateArray.size==0){
            triplet.srcAttr._1.keys.foreach(candidate=>{
              if(triplet.srcAttr._1(candidate)(3)!=triplet.dstId.toInt&&triplet.srcAttr._1(candidate)(1)!=triplet.srcId.toInt&&triplet.srcAttr._1(candidate)(4)>=triplet.srcAttr._1(triplet.srcId.toInt)(4)&&triplet.srcAttr._1(candidate)(1)!=triplet.dstId.toInt){
                candidateArray+=triplet.srcAttr._1(candidate)(1)
              }
            })
          }
          if (candidateArray.size!=0) {
            println("同核c型节点相连"+triplet.srcId+triplet.dstId)
             println(candidateArray(random.nextInt(candidateArray.size)).toString+" "+triplet.dstId.toString )
             (candidateArray(random.nextInt(candidateArray.size)).toString+" "+triplet.dstId.toString )
          }
          else {
            println("同核c型节点相连"+triplet.srcId+triplet.dstId)
            println(triplet.srcId.toString+" "+triplet.dstId.toString)
            (triplet.srcId.toString+" "+triplet.dstId.toString)

          }

        }



        //同核节点相连，存在节点的类型为L型
        //同核节点,挑选出谁为L型
        //L型节点与L型节点相连,先选择正向传播的节点属性
        //选择目的节点正向候选节点属性,节点大于等于源节点（高核节点）, 考虑到同核，目的节点的候选节点不能包含源节点
        else{
          println("同核L型节点相连"+triplet.srcId+triplet.dstId)
          if(triplet.dstAttr._1(triplet.dstId.toInt)(5)>triplet.dstAttr._1(triplet.dstId.toInt)(4)&&triplet.dstAttr._1(triplet.srcId.toInt)(5)>triplet.dstAttr._1(triplet.srcId.toInt)(4)){
            println(1)
            triplet.dstAttr._1.keys.foreach(candidate=>{
              if(triplet.dstAttr._1(candidate)(3)!=triplet.srcId.toInt&&triplet.dstAttr._1(candidate)(1)!=triplet.dstId.toInt&&triplet.dstAttr._1(candidate)(4)>=triplet.dstAttr._2(triplet.dstId.toInt)(4)&&triplet.dstAttr._1(candidate)(1)!=triplet.srcId.toInt){
                candidateArray+=triplet.dstAttr._1(candidate)(1)
              }
            })
            //选择目的节点反向候选节点,节点大于等于源节点（高核节点）
          if(candidateArray.size==0){
              triplet.srcAttr._2.keys.foreach(candidate=>{
              if(triplet.srcAttr._2(candidate)(3)!=triplet.dstId.toInt&&triplet.srcAttr._2(candidate)(1)!=triplet.srcId.toInt&&triplet.srcAttr._2(candidate)(5)>=triplet.srcAttr._1(triplet.srcId.toInt)(4)&&triplet.srcAttr._2(candidate)(1)!=triplet.dstId.toInt){
                candidateArray+=triplet.srcAttr._2(candidate)(1)
              }
            })
               if(candidateArray.size==0){//源节点正向传递
                 triplet.srcAttr._1.keys.foreach(candidate=>{
                   if(triplet.srcAttr._1(candidate)(3)!=triplet.dstId.toInt&&triplet.srcAttr._1(candidate)(1)!=triplet.srcId.toInt&&triplet.srcAttr._1(candidate)(4)>=triplet.srcAttr._1(triplet.srcId.toInt)(4)&&triplet.srcAttr._1(candidate)(1)!=triplet.dstId.toInt){
                     candidateArray+=triplet.srcAttr._1(candidate)(1)
                   }
                 })
                 if(candidateArray.size==0){
                   triplet.dstAttr._2.keys.foreach(candidate=>{
                     if(triplet.dstAttr._2(candidate)(3)!=triplet.srcId.toInt&&triplet.dstAttr._2(candidate)(1)!=triplet.dstId.toInt&&triplet.dstAttr._2(candidate)(4)>=triplet.dstAttr._2(triplet.dstId.toInt)(4)&&triplet.dstAttr._2(candidate)(1)!=triplet.srcId.toInt){
                       candidateArray+=triplet.dstAttr._2(candidate)(1)
                     }
                   })
                   if(candidateArray.size==0){
                     println("同核L型节点相连"+triplet.srcId+triplet.dstId)
                     println("4 "+(triplet.srcId.toString + " " + triplet.dstId.toString))
                     (triplet.srcId.toString+" "+triplet.dstId.toString)
                   }
                   else
                     {
                       triplet.dstAttr._1(triplet.dstId.toInt)(5)-=1
                       triplet.dstAttr._2(triplet.dstId.toInt)(5)-=1
                       println("同核L型节点相连"+triplet.srcId+triplet.dstId)
                       println("4 "+(triplet.srcId.toString + " " + candidateArray(random.nextInt(candidateArray.size - 1)).toString))
                       (triplet.srcId.toString + " " + candidateArray(random.nextInt(candidateArray.size - 1)).toString)
                       }
                 }
                 else{
                   triplet.srcAttr._1(triplet.srcId.toInt)(5)-=1
                   triplet.srcAttr._2(triplet.srcId.toInt)(5)-=1
                   println("同核L型节点相连"+triplet.srcId+triplet.dstId)
                   println("5 "+candidateArray(random.nextInt(candidateArray.size - 1)).toString+" "+triplet.dstId.toString )
                   (candidateArray(random.nextInt(candidateArray.size - 1)).toString+" "+triplet.dstId.toString )
                   }
                }
               else{
                 triplet.srcAttr._1(triplet.srcId.toInt)(5)-=1
                 triplet.srcAttr._2(triplet.srcId.toInt)(5)-=1
                 println("同核L型节点相连"+triplet.srcId+triplet.dstId)
               println("6 "+candidateArray(random.nextInt(candidateArray.size )).toString+" "+triplet.dstId.toString )
               (candidateArray(random.nextInt(candidateArray.size )).toString+" "+triplet.dstId.toString )
               }
          }
          else{
            triplet.dstAttr._1(triplet.dstId.toInt)(5)-=1
            triplet.dstAttr._2(triplet.dstId.toInt)(5)-=1
            println("同核L型节点相连"+triplet.srcId+triplet.dstId)
             println("7 "+triplet.srcId.toString+" "+candidateArray(random.nextInt(candidateArray.size)).toString )
             (triplet.srcId.toString+" "+candidateArray(random.nextInt(candidateArray.size)).toString )
          }
          } //两个都是L型


            //目的节点为L型
          //选择目的节点正向候选节点属性,节点大于等于源节点（高核节点）, 考虑到同核，目的节点的候选节点不能包含源节点
          else if(triplet.dstAttr._1(triplet.dstId.toInt)(5)>triplet.dstAttr._1(triplet.dstId.toInt)(4)){

            triplet.dstAttr._1.keys.foreach(candidate=>{
              if(triplet.dstAttr._1(candidate)(3)!=triplet.srcId.toInt&&triplet.dstAttr._1(candidate)(1)!=triplet.dstId.toInt&&triplet.dstAttr._1(candidate)(4)>=triplet.dstAttr._1(triplet.dstId.toInt)(4)&&triplet.dstAttr._1(candidate)(1)!=triplet.srcId.toInt){
                candidateArray+=triplet.dstAttr._1(candidate)(1)
              }
            })
            //反向候选节点不够，反向
            if(candidateArray.size==0){
              triplet.dstAttr._2.keys.foreach(candidate=>{
                if(triplet.dstAttr._2(candidate)(3)!=triplet.srcId.toInt&&triplet.dstAttr._2(candidate)(1)!=triplet.dstId.toInt&&triplet.dstAttr._2(candidate)(4)>=triplet.dstAttr._2(triplet.dstId.toInt)(4)&&triplet.dstAttr._2(candidate)(1)!=triplet.srcId.toInt){
                  candidateArray+=triplet.dstAttr._2(candidate)(1)
                }
              })
            }
            if (candidateArray.size!=0) {
              triplet.dstAttr._1(triplet.dstId.toInt)(5)-=1
              triplet.dstAttr._2(triplet.dstId.toInt)(5)-=1
              println("同核L型节点相连"+triplet.srcId+triplet.dstId)
              println(" 8 "+triplet.srcId.toString+" "+candidateArray(random.nextInt(candidateArray.size)).toString )
              (triplet.srcId.toString+" "+candidateArray(random.nextInt(candidateArray.size)).toString )

            }
            else {
              println("同核L型节点相连"+triplet.srcId+triplet.dstId)
              println(" 9 "+triplet.srcId.toString+" "+triplet.dstId.toString)
              (triplet.srcId.toString+" "+triplet.dstId.toString)
            }

          }

            //同核L型相连之源节点为L型
          //选择目的节点反向候选节点,节点大于等于源节点（高核节点）
          else{

            triplet.srcAttr._2.keys.foreach(candidate=>{
              if(triplet.srcAttr._2(candidate)(3)!=triplet.dstId.toInt&&triplet.srcAttr._2(candidate)(1)!=triplet.srcId.toInt&&triplet.srcAttr._2(candidate)(4)>=triplet.srcAttr._2(triplet.srcId.toInt)(4)&&triplet.srcAttr._2(candidate)(1)!=triplet.dstId.toInt){
                candidateArray+=candidate
              }
            })
            //选择正向节点
            if(candidateArray.size==0){
              triplet.srcAttr._1.keys.foreach(candidate=>{
                if(triplet.srcAttr._1(candidate)(3)!=triplet.dstId.toInt&&triplet.srcAttr._1(candidate)(1)!=triplet.srcId.toInt&&triplet.srcAttr._1(candidate)(4)>=triplet.srcAttr._1(triplet.srcId.toInt)(4)&&triplet.srcAttr._1(candidate)(1)!=triplet.dstId.toInt){
                  candidateArray+=triplet.srcAttr._1(candidate)(1)
                }
              })
            }
            if (candidateArray.size!=0) {
              triplet.srcAttr._1(triplet.srcId.toInt)(5)-=1
              triplet.srcAttr._2(triplet.srcId.toInt)(5)-=1
              println("同核L型节点相连"+triplet.srcId+triplet.dstId)
             println("10 "+candidateArray(random.nextInt(candidateArray.size )).toString+" "+triplet.dstId.toString )
             (candidateArray(random.nextInt(candidateArray.size )).toString+" "+triplet.dstId.toString )
            }
            else {
              println("同核L型节点相连"+triplet.srcId+triplet.dstId)
              println("11 "+triplet.srcId.toString+" "+triplet.dstId.toString)
              (triplet.srcId.toString+" "+triplet.dstId.toString)
            }
          }
        }//同核L型

      }


    })

    /*println(ResultArray.size)

    verticeOutput.outputGraph(ResultArray)*/
    resultLastGraph


    //newGraph.vertices.saveAsTextFile("/user/output/Kcore09")

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
