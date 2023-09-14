package io.hydrolix.splunk

import io.hydrolix.spark.model.JSON

import com.clickhouse.logging.LoggerFactory
import com.github.tototoshi.csv.CSVWriter
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.types.{DataType, DataTypes, DecimalType, StructType}

import java.io.{ByteArrayInputStream, ByteArrayOutputStream}
import java.util.UUID
import java.util.concurrent.LinkedBlockingQueue
import scala.util.Using.resource
import scala.util.control.Breaks.{break, breakable}

/**
 * A thread that reads rows from a queue, and writes them to stdout in chunks of 50000 rows at a time.
 * There should only be one of these, to make sure our output is sequentially consistent.
 */
class OutputWriterThread(workerId: UUID, qp: QueryPlan, rowQ: LinkedBlockingQueue[InternalRow]) extends Thread {
  private val log = LoggerFactory.getLogger(getClass)

  override def run(): Unit = {
    var rowCount = 0
    var chunkCount = 0
    var chunksDone = false

    // For each chunk...
    while (!chunksDone) {
      chunkCount += 1
      val baos = new ByteArrayOutputStream(1024*1024)
      resource(CSVWriter.open(baos)) { writer =>
        // Write chunk header row
        writer.writeRow(qp.cols.map { col =>
          if (col.name == qp.primaryKeyField) {
            "_time"
          } else {
            col.name
          }
        })

        // For each row...
        breakable {
          while (true) {
            val row = rowQ.take()

            if (row eq HdxQueryCommand.outputDoneSignal) {
              log.info(s"WORKER-$workerId:WRITER: Finished writing output: $rowCount rows, $chunkCount chunks")
              flush(baos, finished = true)
              baos.reset()
              chunksDone = true
              break()
            }

            rowCount += 1

            writer.writeRow(rowToCsv(qp.cols, row))

            if (rowCount % 50000 == 0) {
              log.info(s"WORKER-$workerId:WRITER: Chunk #$chunkCount done")
              flush(baos, finished = false)
              baos.reset()
              break()
            }
          }
        }
      }
    }
  }

  private def flush(baos: ByteArrayOutputStream, finished: Boolean): Unit = {
    writeChunk(
      System.out,
      JSON.objectMapper.writeValueAsBytes(ExecuteResponseMeta(finished)),
      Some((
        baos.size(),
        new ByteArrayInputStream(baos.toByteArray)
      ))
    )

    if (!finished) {
      readChunk(System.in) match {
        case (Some(meta), None) =>
          log.info(s"WORKER-$workerId:WRITER: Next input chunk metadata is $meta")
        case (None, _) =>
          log.warn(s"WORKER-$workerId:WRITER: Splunkd hung up on us but we don't think we're done!")
      }
    }
  }

  private def rowToCsv(schema: StructType, row: InternalRow): List[String] = {
    for ((field, i) <- schema.fields.zipWithIndex.toList) yield {
      get(row, i, field.dataType)
    }
  }

  private def get(row: InternalRow, i: Int, typ: DataType): String = {
    if (row.isNullAt(i)) {
      null
    } else typ match {
      case DataTypes.BooleanType => row.getBoolean(i).toString
      case DataTypes.StringType => row.getString(i)
      case DataTypes.LongType => row.getLong(i).toString
      case DataTypes.IntegerType => row.getInt(i).toString
      case DataTypes.ShortType => row.getShort(i).toString
      case DataTypes.ByteType => row.getByte(i).toString
      case DataTypes.DoubleType => row.getDouble(i).toString
      case DataTypes.FloatType => row.getFloat(i).toString
      case DataTypes.TimestampType | DataTypes.TimestampNTZType =>
        // TODO maybe make this conditional, e.g. primary vs. other timestamp fields
        val micros = row.getLong(i)
        (BigDecimal(micros) / 1000000).toString()
      case dt: DecimalType =>
        row.getDecimal(i, dt.precision, dt.scale).toString()
      case other =>
        // TODO arrays, maps
        sys.error(s"Can't serialize $other values")
    }
  }
}
