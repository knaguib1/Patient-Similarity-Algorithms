/**
 *
 * students: please put your implementation in this file!
 */
package edu.gatech.cse6250.jaccard

import edu.gatech.cse6250.model._
import edu.gatech.cse6250.model.{ EdgeProperty, VertexProperty }
import org.apache.spark.graphx._
import org.apache.spark.rdd.RDD

object Jaccard {

  def jaccardSimilarityOneVsAll(graph: Graph[VertexProperty, EdgeProperty], patientID: Long): List[Long] = {
    /**
     * Given a patient ID, compute the Jaccard similarity w.r.t. to all other patients.
     * Return a List of top 10 patient IDs ordered by the highest to the lowest similarity.
     * For ties, random order is okay. The given patientID should be excluded from the result.
     */

    /** Remove this placeholder and implement your code */
    // collect nieghboring vertices using lookup method
    // https://stackoverflow.com/questions/45111023/collecting-neighboring-vertices-graphx

    val neighbors = graph
      .collectNeighborIds(EdgeDirection.Out)
      .lookup(patientID)
      .flatten
      .toSet

    val filteredGraph = graph
      .subgraph(vpred = {case(id, attr) =>
        attr.isInstanceOf[PatientProperty]
      })
      .collectNeighborIds(EdgeDirection.Out)
      .map(_._1)
      .filter(vertex => vertex != patientID)
      .collect()
      .toSet

    val patientNeighbors = graph
      .collectNeighborIds(EdgeDirection.Out)
      .filter(vertex => filteredGraph.contains(vertex._1))

    val jaccardSimilarity = patientNeighbors
      .map { vertex =>
        // calculate similarity
        val similarity = jaccard(neighbors, vertex._2.toSet)

        // need patientID's
        (similarity, vertex._1)
      }

    jaccardSimilarity
      .sortBy(_._1,false) 
      .take(10) 
      .map(_._2.toLong) 
      .toList 

  }

  def jaccardSimilarityAllPatients(graph: Graph[VertexProperty, EdgeProperty]): RDD[(Long, Long, Double)] = {
    /**
     * Given a patient, med, diag, lab graph, calculate pairwise similarity between all
     * patients. Return a RDD of (patient-1-id, patient-2-id, similarity) where
     * patient-1-id < patient-2-id to avoid duplications
     */

    /** Remove this placeholder and implement your code */

    val filteredGraph = graph
      .subgraph(vpred = {case(id, attr) =>
        attr.isInstanceOf[PatientProperty]
      })
      .collectNeighborIds(EdgeDirection.Out)
      .map(_._1)
      .collect()
      .toSet

    val patientNeighbors = graph
      .collectNeighborIds(EdgeDirection.Out)
      .filter(vertex => filteredGraph.contains(vertex._1))

    val cartesianNeighbors = patientNeighbors
      .cartesian(patientNeighbors)
      .filter {
        case(vertex1, vertex2) => vertex1._1 < vertex2._1
      }

    val jaccardSimilarity = cartesianNeighbors
      .map { case(vertex1, vertex2) =>
        // calculate similarity
        val similarity = jaccard(vertex1._2.toSet, vertex2._2.toSet)

        (vertex1._1, vertex2._1, similarity)
      }

    jaccardSimilarity

  }

  def jaccard[A](a: Set[A], b: Set[A]): Double = {
    /**
     * Helper function
     *
     * Given two sets, compute its Jaccard similarity and return its result.
     * If the union part is zero, then return 0.
     */

    /** Remove this placeholder and implement your code */

    val union_part = a.union(b).size.toDouble

    if (union_part == 0){
      return 0.0
    }

    a.intersect(b).size.toDouble / union_part

  }
}
