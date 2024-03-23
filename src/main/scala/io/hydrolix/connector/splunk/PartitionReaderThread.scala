package io.hydrolix.connector.splunk

import java.util.UUID
import java.util.concurrent.LinkedBlockingQueue
import scala.util.control.Breaks.{break, breakable}

import com.clickhouse.logging.LoggerFactory

import io.hydrolix.connectors.data.{CoreRowAdapter, Row}
import io.hydrolix.connectors.partitionreader.RowPartitionReader
import io.hydrolix.connectors.types.StringType
import io.hydrolix.connectors.{HdxConnectionInfo, HdxPartitionScanPlan, microsToInstant}

/**
 * A thread that repeatedly gets a [[HdxPartitionScanPlan]] from a job queue, runs the scan using [[RowPartitionReader]],
 * and sends the rows from the partition reader to the output row queue, where they'll be consumed by
 * [[OutputWriterThread]].
 */
final class PartitionReaderThread(workerId: UUID,
                                  threadNo: Int,
                                      info: HdxConnectionInfo,
                                        qp: QueryPlan,
                                      jobQ: LinkedBlockingQueue[HdxPartitionScanPlan],
                                      rowQ: LinkedBlockingQueue[Row])
  extends Thread
{
  private val log = LoggerFactory.getLogger(getClass)

  private val timestampPos = qp.cols.fields.indexWhere(_.name == qp.primaryKeyField)
  private val otherArgPoss = qp.otherTerms.map {
    case (name, value) =>
      val pos = qp.cols.fields.indexWhere(_.name == name)
      val typ = qp.cols.fields(pos).`type`
      if (typ != StringType) sys.error(s"Can't search for $name of type $typ (only Strings)")
      pos -> value
  }

  override def run(): Unit = {
    // For each HdxScanPartition taken from jobQ...
    breakable {
      while (true) {
        val scan = jobQ.take()

        if (scan eq HdxQueryCommand.partitionsDoneSignal) {
          log.info(s"WORKER-$workerId:READER-$threadNo: Received done signal, no more partitions to scan")
          break()
        }

        log.info(s"WORKER-$workerId:READER-$threadNo: Scanning partition $scan...")

        val storage = qp.storages.getOrElse(scan.storageId, sys.error(s"Unknown storage #${scan.storageId}"))

        val hdxReader = new RowPartitionReader(info, storage, scan, CoreRowAdapter, Row.empty)

        // For each row from the partition reader...
        hdxReader.iterator.forEachRemaining { row =>
          val rowTimestamp = microsToInstant(row.getLong(timestampPos))

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
