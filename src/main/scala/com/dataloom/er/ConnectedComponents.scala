package com.dataloom.er

import org.apache.spark.rdd.RDD
import scala.collection.mutable.HashSet

object ConnectedComponents {

  /**
    * Applies Small Star operation on RDD of nodePairs
    *
    * @param nodePairs on which to apply Small Star operations
    * @return new nodePairs after the operation and conncectivy change count
    */
  private def smallStar(nodePairs: RDD[(Long, Long)]): (RDD[(Long, Long)]) = {

    /**
      * generate RDD of (self, List(neighbors)) where self > neighbors
      * E.g.: nodePairs (1, 4), (6, 1), (3, 2), (6, 5)
      * will result into (4, List(1)), (6, List(1)), (3, List(2)), (6, List(5))
      */
    val neighbors = nodePairs.map(x => {
      val (self, neighbor) = (x._1, x._2)
      if (self > neighbor)
        (self, List(neighbor))
      else
        (neighbor, List(self))
    })

    /**
      * reduce on self to get list of all its neighbors.
      * E.g: (4, List(1)), (6, List(1)), (3, List(2)), (6, List(5))
      * will result into (4, List(1)), (6, List(1, 5)), (3, List(2))
      * Note:
      * (1) you may need to tweak number of partitions.
      * (2) also, watch out for data skew. In that case, consider using rangePartitioner
      */

    val allNeighbors = neighbors.reduceByKey((a, b) => {
      b ::: a
    })

    /**
      * Apply Small Star operation on (self, List(neighbor)) to get newNodePairs and count the change in connectivity
      */

    allNeighbors.flatMap(x => {
      val self = x._1
      val neighbors = x._2.distinct
      val minNode = argMin(self :: neighbors)
      val newNodePairs = (self :: neighbors).map(neighbor => {
        (neighbor, minNode)
      }).filter(x => {
        val neighbor = x._1
        val minNode = x._2
        ((neighbor <= self && neighbor != minNode) || (self == neighbor))
      })

      newNodePairs.toSet.toList
    })
  }

  /**
    * Apply Large Star operation on a RDD of nodePairs
    *
    * @param nodePairs on which to apply Large Star operations
    * @return new nodePairs after the operation and conncectivy change count
    */
  private def largeStar(nodePairs: RDD[(Long, Long)]): (RDD[(Long, Long)]) = {

    /**
      * generate RDD of (self, List(neighbors))
      * E.g.: nodePairs (1, 4), (6, 1), (3, 2), (6, 5)
      * will result into (4, List(1)), (1, List(4)), (6, List(1)), (1, List(6)), (3, List(2)), (2, List(3)), (6, List(5)), (5, List(6))
      */

    val neighbors = nodePairs.flatMap(x => {
      val (self, neighbor) = (x._1, x._2)
      if (self == neighbor)
        List((self, neighbor))
      else
        List((self, neighbor), (neighbor, self))
    })

    /**
      * reduce on self to get list of all its neighbors.
      * We are using aggregateByKey. You may choose to use reduceByKey as in the Small Star
      * E.g: (4, List(1)), (1, List(4)), (6, List(1)), (1, List(6)), (3, List(2)), (2, List(3)), (6, List(5)), (5, List(6))
      * will result into (4, List(1)), (1, List(4, 6)), (6, List(1, 5)), (3, List(2)), (2, List(3)), (5, List(6))
      * Note:
      * (1) you may need to tweak number of partitions.
      * (2) also, watch out for data skew. In that case, consider using rangePartitioner
      */

    val localAdd = (s: HashSet[Long], v: Long) => s += v
    val partitionAdd = (s1: HashSet[Long], s2: HashSet[Long]) => s1 ++= s2
    val allNeighbors = neighbors.aggregateByKey(HashSet.empty[Long] /*, rangePartitioner*/)(localAdd, partitionAdd)

    /**
      * Apply Large Star operation on (self, List(neighbor)) to get newNodePairs and count the change in connectivity
      */

    allNeighbors.flatMap(x => {
      val self = x._1
      val neighbors = x._2.toList
      val minNode = argMin(self :: neighbors)
      val newNodePairs = (self :: neighbors).map(neighbor => {
        (neighbor, minNode)
      }).filter(x => {
        val neighbor = x._1
        val minNode = x._2
        ((neighbor > self && neighbor != minNode) || (neighbor == self))
      })

      newNodePairs.toSet.toList
    })
  }

  private def argMin(nodes: List[Long]): Long = {
    nodes.min(Ordering.by((node: Long) => node))
  }

  /**
    * Implements alternatingAlgo.  Converges when the changeCount is either 0 or does not change from the previous iteration
    *
    * @param nodePairs          for a graph
    * @param currIterationCount counter to capture number of iterations
    * @param maxIterationCount  maximum number iterations to try before giving up
    * @return RDD of nodePairs
    */
  private def connectedComponents(nodePairs: RDD[(Long, Long)], currIterationCount: Int, maxIterationCount: Int): (RDD[(Long, Long)]) = {

    val iterationCount = currIterationCount + 1
    if (currIterationCount >= maxIterationCount) {
      nodePairs
    }
    else {

      val nodePairsLargeStar = largeStar(nodePairs)
      val nodePairsSmallStar = smallStar(nodePairsLargeStar)

      connectedComponents(nodePairsSmallStar, iterationCount, maxIterationCount)
    }

  }

  /**
    * Group profiles together by running connected components across an RDD of profiles.
    *
    * @param edges       RDD of edges
    * @param iterations     number of passes over the graph
    * @return RDD of Tuple2 with the first value the lowest common id of the connected pairs and the last the id of the profile.
    */
  def groupEdges(edges: RDD[(Long, Long)], iterations: Int): RDD[(Long, Long)] = connectedComponents(edges, 0, iterations)


}
