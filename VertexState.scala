package Experiment_Kcore_20190422

import scala.collection.mutable.ArrayBuffer

/**
 * Louvain vertex state
 * Contains all information needed for louvain community detection
 */
class VertexState extends Serializable{


  var RNTE=Map[Int,Array[Int]]()

 override def toString: String = super.toString
}