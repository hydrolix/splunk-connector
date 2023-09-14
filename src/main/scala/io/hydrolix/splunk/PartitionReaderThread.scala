package io.hydrolix.splunk

import io.hydrolix.spark.connector.HdxScanPartition
import io.hydrolix.spark.connector.partitionreader.RowPartitionReader
import io.hydrolix.spark.model.HdxConnectionInfo
import io.hydrolix.splunk.HdxQueryCommand.partitionsDoneSignal

import com.clickhouse.logging.LoggerFactory
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.util.DateTimeUtils
import org.apache.spark.sql.types.DataTypes

import java.util.UUID
import java.util.concurrent.LinkedBlockingQueue
import scala.util.control.Breaks.{break, breakable}

/**
 * A thread that repeatedly gets a [[HdxScanPartition]] from a job queue, runs the scan using [[RowPartitionReader]],
 * and sends the rows from the partition reader to the output row queue, where they'll be consumed by
 * [[OutputWriterThread]].
 */
final class PartitionReaderThread(workerId: UUID,
                                  threadNo: Int,
                                      info: HdxConnectionInfo,
                                        qp: QueryPlan,
                                      jobQ: LinkedBlockingQueue[HdxScanPartition],
                                      rowQ: LinkedBlockingQueue[InternalRow])
  extends Thread
{
  private val log = LoggerFactory.getLogger(getClass)

  private val timestampPos = qp.cols.fieldIndex(qp.primaryKeyField)
  private val otherArgPoss = qp.otherTerms.map {
    case (name, value) =>
      val pos = qp.cols.fieldIndex(name)
      val typ = qp.cols.fields(pos).dataType
      if (typ != DataTypes.StringType) sys.error(s"Can't search for $name of type $typ (only Strings)")
      pos -> value
  }

  override def run(): Unit = {
    // For each HdxScanPartition taken from jobQ...
    breakable {
      while (true) {
        val scan = jobQ.take()

        if (scan eq partitionsDoneSignal) {
          log.info(s"WORKER-$workerId:READER-$threadNo: Received done signal, no more partitions to scan")
          break()
        }

        log.info(s"WORKER-$workerId:READER-$threadNo: Scanning partition $scan...")

        val storage = qp.storages.getOrElse(scan.storageId, sys.error(s"Unknown storage #${scan.storageId}"))

        val hdxReader = new RowPartitionReader(info, storage, qp.primaryKeyField, scan)

        // For each row from the partition reader...
        while (hdxReader.next()) {
          val row = hdxReader.get()
          val rowTimestamp = DateTimeUtils.microsToInstant(row.getLong(timestampPos))

          // Check the row timestamp is actually in the time bounds
          if (rowTimestamp.compareTo(qp.minTimestamp) >= 0 && rowTimestamp.compareTo(qp.maxTimestamp) <= 0) {
            // Check other fields are as expected
            val otherValuesMatch = otherArgPoss.forall {
              case (pos, "null") =>
                // Special-case "foo=null"
                row.isNullAt(pos)
              case (pos, value) =>
                if (row.isNullAt(pos)) {
                  false
                } else {
                  row.getString(pos) == value
                }
            }

            if (otherValuesMatch) {
              // This row satisfies the predicates; send it
              rowQ.put(row)
            }
          }
        }

        hdxReader.close()
      }
    }
  }
}
