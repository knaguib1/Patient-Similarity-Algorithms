/**
 * @author Hang Su <hangsu@gatech.edu>.
 */

package edu.gatech.cse6250.graphconstruct

import edu.gatech.cse6250.model._
import org.apache.spark.graphx._
import org.apache.spark.rdd.RDD

object GraphLoader {
  /**
   * Generate Bipartite Graph using RDDs
   *
   * @input: RDDs for Patient, LabResult, Medication, and Diagnostic
   * @return: Constructed Graph
   *
   * NOTICE: please use the predefined attribute name (patientVertex, labEdge, etc.)
   * in your implementation of GraphLoader Object or you may get points off
   * Example of patientVertex and patientEdge is shown below.
   *
   * labEdge, medicationEdge and diagnosticEdge are bidirectional
   *
   */
  var patientVertex: RDD[(VertexId, VertexProperty)] = _
  var labResultVertex: RDD[(VertexId, VertexProperty)] = _
  var medicationVertex: RDD[(VertexId, VertexProperty)] = _
  var diagnosticVertex: RDD[(VertexId, VertexProperty)] = _

  var labEdge: RDD[Edge[EdgeProperty]] = _
  var medicationEdge: RDD[Edge[EdgeProperty]] = _
  var diagnosticEdge: RDD[Edge[EdgeProperty]] = _

  def load(patients: RDD[PatientProperty], labResults: RDD[LabResult],
    medications: RDD[Medication], diagnostics: RDD[Diagnostic]): Graph[VertexProperty, EdgeProperty] = {
    /**
     *
     */
    import org.apache.spark.sql.{ DataFrame, SparkSession }

    val sc = patients.sparkContext
    var nStartIndex :Long = 0

    /** HINT: See Example of Making Patient Vertices Below */
    patientVertex = patients.map(patient => (patient.patientID.toLong, patient.asInstanceOf[VertexProperty]))

    // start index for labResultVertex
    val nPatients = patients.map(_.patientID).distinct().count()

    // labResultVertex
    // latest labResults
    val labResultsLatest = labResults
      .map(lab => ((lab.patientID, lab.labName), lab))
      .reduceByKey((lab1, lab2) => if(lab1.date > lab2.date) lab1 else lab2)
      .map{case(key, value) => value}

    val labResultsVertexIdRDD = labResultsLatest
      .map(_.labName)
      .distinct()
      .zipWithIndex()
      .map{ case(labName, zeroBasedIndex) =>
        (labName, zeroBasedIndex + nPatients + 1)
      }

    val lab2VertexId = labResultsVertexIdRDD.collect.toMap

    labResultVertex = labResultsVertexIdRDD
      .map{ case(labName, index) =>
        val labVertex = LabResultProperty(labName)
        (index, labVertex.asInstanceOf[VertexProperty])
      }

    // start index for medicationVertex
    val nLabs = lab2VertexId.size

    // medicationVertex
    // latest medications
    val medLatest = medications
      .map(med => ((med.patientID, med.medicine), med))
      .reduceByKey((med1, med2) => if(med1.date > med2.date) med1 else med2)
      .map{case(key, value) => value}

    val medicationVertexIdRDD = medLatest
      .map(_.medicine)
      .distinct()
      .zipWithIndex()
      .map{ case(medicine, zeroBasedIndex) =>
        (medicine, zeroBasedIndex + nPatients + nLabs + 1)
      }

    val med2VertexId = medicationVertexIdRDD.collect.toMap

    medicationVertex = medicationVertexIdRDD
      .map{ case(medicine, index) =>
        val medVertex = MedicationProperty(medicine)
        (index, medVertex.asInstanceOf[VertexProperty])
      }

    // start index for diagnosticVertex
    val nMeds = med2VertexId.size

    // diagnosticVertex
    // diagnostic latest
    val diagLatest = diagnostics
      .map(diag => ((diag.patientID, diag.icd9code), diag))
      .reduceByKey((diag1, diag2) => if(diag1.date > diag2.date) diag1 else diag2)
      .map{case(key, value) => value}

    val diagnosticVertexIdRDD = diagLatest
      .map(_.icd9code)
      .distinct()
      .zipWithIndex()
      .map{ case(icd9code, zeroBasedIndex) =>
        (icd9code, zeroBasedIndex + nPatients + nLabs + nMeds + 1)
      }

    val diag2VertexId = diagnosticVertexIdRDD.collect.toMap

    diagnosticVertex = diagnosticVertexIdRDD
      .map{ case(icd9code, index) =>
        val diagVertex = DiagnosticProperty(icd9code)
        (index, diagVertex.asInstanceOf[VertexProperty])
      }

    /**
     * HINT: See Example of Making PatientPatient Edges Below
     *
     * This is just sample edges to give you an example.
     * You can remove this PatientPatient edges and make edges you really need
     */
    case class PatientPatientEdgeProperty(someProperty: SampleEdgeProperty) extends EdgeProperty
    val patientPatientEdge: RDD[Edge[EdgeProperty]] = patients
      .map({ p =>
        Edge(p.patientID.toLong, p.patientID.toLong, SampleEdgeProperty("sample").asInstanceOf[EdgeProperty])
      })

    val bcLab2VertexId = sc.broadcast(lab2VertexId)
    val bcMed2VertexId = sc.broadcast(med2VertexId)
    val bcDiag2VertexId = sc.broadcast(diag2VertexId)

    // creating bi-directional graph in two steps
    // edges based on patient --> lab, med, diag
    // edges based on lab, med, diag --> patient

    // labEdge
    val patientLabEdge = labResultsLatest
      .map(lab => ((lab.patientID, lab.labName), lab))
      .map{case((patientID, labName), lab) => Edge(
         patientID.toLong,
         bcLab2VertexId.value(labName),
         PatientLabEdgeProperty(lab).asInstanceOf[EdgeProperty]
        )
      }

    val labPatientEdge = labResultsLatest
      .map(lab => ((lab.patientID, lab.labName), lab))
      .map{case((patientID, labName), lab) => Edge(
          bcLab2VertexId.value(labName),
          patientID.toLong,
          PatientLabEdgeProperty(lab).asInstanceOf[EdgeProperty]
        )
      }

    labEdge = sc.union(patientLabEdge, labPatientEdge)

    // medicationEdge
    val patientMedEdge = medLatest
      .map(med => ((med.patientID, med.medicine), med))
      .map{case((patientID, medicine), med) => Edge(
          patientID.toLong,
          bcMed2VertexId.value(medicine),
          PatientMedicationEdgeProperty(med).asInstanceOf[EdgeProperty]
        )
      }

    val medPatientEdge = medLatest
      .map(med => ((med.patientID, med.medicine), med))
      .map{case((patientID, medicine), med) => Edge(
          bcMed2VertexId.value(medicine),
          patientID.toLong,
          PatientMedicationEdgeProperty(med).asInstanceOf[EdgeProperty]
        )
      }

    medicationEdge = sc.union(patientMedEdge, medPatientEdge)

    // diagnosticEdge
    val patientDiagEdge = diagLatest
      .map(diag => ((diag.patientID, diag.icd9code), diag))
      .map{case((patientID, icd9code), diag) => Edge(
          patientID.toLong,
          bcDiag2VertexId.value(icd9code),
          PatientDiagnosticEdgeProperty(diag).asInstanceOf[EdgeProperty]
        )
      }

    val diagPatientEdge = diagLatest
      .map(diag => ((diag.patientID, diag.icd9code), diag))
      .map{case((patientID, icd9code), diag) => Edge(
          bcDiag2VertexId.value(icd9code),
          patientID.toLong,
          PatientDiagnosticEdgeProperty(diag).asInstanceOf[EdgeProperty]
        )
      }

    diagnosticEdge = sc.union(patientDiagEdge, diagPatientEdge)

    // Making Graph
    val vertices = sc.union(patientVertex, labResultVertex, medicationVertex, diagnosticVertex)
    val edges = sc.union(labEdge, medicationEdge, diagnosticEdge)

    val graph: Graph[VertexProperty, EdgeProperty] = Graph[VertexProperty, EdgeProperty](vertices, edges)

    graph
  }
}
