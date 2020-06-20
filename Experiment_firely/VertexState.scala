package Experiment_firely

import scala.collection.mutable.ArrayBuffer

/**
 * Louvain vertex state
 * Contains all information needed for louvain community detection
 */
class VertexState extends Serializable{

  var community = -1L
  var nodecore=0L
  var nodeEffect=0L
  var nodeFireL=0F
  val step=0L
  var changed = false
  var neighborArray=Map[Int,Array[Int]]()
  var arrayCandidate=ArrayBuffer[Any]()
 // var resultArray =Map[Int,Array[Int]]()


  override def toString = s"VertexState($community, $nodecore, $nodeEffect, $nodeFireL, $changed)"
}