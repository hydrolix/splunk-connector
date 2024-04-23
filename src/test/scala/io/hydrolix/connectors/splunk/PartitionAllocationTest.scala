package io.hydrolix.connectors.splunk

import org.junit.Assert.assertEquals
import org.junit.Test

import java.util.UUID
import scala.collection.mutable

class PartitionAllocationTest {
  @Test
  def testPartitionAllocation(): Unit = {
    val paths = List.fill(23) { s"path-${UUID.randomUUID().toString}" }
    val workers = List.fill(7) { s"worker-${UUID.randomUUID()}" }
    val numWorkers = workers.size

    val partitionsPerWorker = mutable.Map[String, Vector[String]]().withDefaultValue(Vector())

    // Allocate every partition that needs to be scanned to one of the workers
    for ((part, i) <- paths.zipWithIndex) {
      val worker = workers(i % numWorkers)
      val existing = partitionsPerWorker(worker)
      partitionsPerWorker.update(worker, existing :+ part)
    }

    println(partitionsPerWorker.mkString("\n"))

    val allParts = partitionsPerWorker.values.map(_.size).sum
    assertEquals("all paths are allocated", paths.size, allParts)
    assertEquals("every worker has at least one path", workers.size, partitionsPerWorker.keys.size)
  }
}
