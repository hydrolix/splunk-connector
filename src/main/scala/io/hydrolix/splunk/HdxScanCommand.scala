package io.hydrolix.splunk

import io.hydrolix.spark.connector.HdxPartitionReader
import io.hydrolix.spark.model.JSON

import com.github.tototoshi.csv.CSVWriter
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.util.DateTimeUtils
import org.apache.spark.sql.types.{DataType, DataTypes, DecimalType, StructType}
import org.slf4j.LoggerFactory

import java.io.{File, FileInputStream, FileOutputStream}
import java.net.URI
import java.time.ZoneOffset
import java.time.format.DateTimeFormatter

object HdxScanCommand {
  private val logger = LoggerFactory.getLogger(getClass)

  def main(args: Array[String]): Unit = {
    val getInfoMeta = readInitialChunk(System.in)
    logger.info(s"SCAN getinfo metadata: $getInfoMeta")

    val resp = GetInfoResponseMeta(
      CommandType.streaming,
      generating = true,
      Nil,
      Some(600),
      "",
      finished = false,
      "",
      InspectorMessages(Nil)
    )

    writeChunk(System.out, JSON.objectMapper.writeValueAsBytes(resp), None)

    readChunk(System.in) match {
      case (None, _) => sys.exit(0)
      case (Some(execMeta), execData) =>
        val info = Config.loadWithSessionKey(new URI(getInfoMeta.searchInfo.splunkdUri), getInfoMeta.searchInfo.sessionKey)
        logger.info(s"SCAN config loaded: ${info.toString}")

        val (dbName, tableName) = getDbTableArg(getInfoMeta)

        val cat = tableCatalog(info)

        val table = hdxTable(cat, dbName, tableName)

        val cols = getRequestedCols(getInfoMeta, table)
        logger.info(s"SCAN requested columns: $cols")

        val minTimestamp = DateTimeUtils.microsToInstant((getInfoMeta.searchInfo.earliestTime * 1000000).toLong)
        val maxTimestamp = DateTimeUtils.microsToInstant((getInfoMeta.searchInfo.latestTime * 1000000).toLong)

        val otherTerms = getOtherTerms(getInfoMeta).toMap

        val partitions = planPartitions(table, cols, minTimestamp, maxTimestamp, otherTerms, info)

        // TODO do output in 50k chunks over multiple iterations, not just a single spew
        // TODO do output in 50k chunks over multiple iterations, not just a single spew
        // TODO do output in 50k chunks over multiple iterations, not just a single spew
        // TODO do output in 50k chunks over multiple iterations, not just a single spew

        val tmp = File.createTempFile("hdx_output", ".csv")
        tmp.deleteOnExit()
        val writer = CSVWriter.open(new FileOutputStream(tmp))
        writer.writeRow(cols.map(_.name))

        var count = 0
        var written = 0
        for (partition <- partitions) {
          val timestampPos = partition.schema.fieldIndex(table.primaryKeyField)
          val otherArgPoss = otherTerms.map {
            case (name, value) =>
              val pos = partition.schema.fieldIndex(name)
              val typ = partition.schema.fields(pos).dataType
              if (typ != DataTypes.StringType) sys.error(s"Can't search for $name of type $typ (only Strings)")
              pos -> value
          }

          val pr = new HdxPartitionReader(info, table.storage, table.primaryKeyField, partition)
          while (pr.next()) {
            val row = pr.get()
            count += 1
            val rowTimestamp = DateTimeUtils.microsToInstant(row.getLong(timestampPos))

            if (rowTimestamp.compareTo(minTimestamp) >= 0 && rowTimestamp.compareTo(maxTimestamp) <= 0) {
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
                written += 1
                writer.writeRow(rowToCsv(cols, row))
              }
            }
          }
          pr.close()
        }

        logger.info(s"SCAN scanned $count records; written $written (${count - written} filtered out)")

        writer.close()
        val dataLen = tmp.length()
        val execResp = ExecuteResponseMeta(true)
        writeChunk(System.out, JSON.objectMapper.writeValueAsBytes(execResp), Some((dataLen.toInt, new FileInputStream(tmp))))
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
        val micros = row.getLong(i)
        val inst = DateTimeUtils.microsToInstant(micros)
        DateTimeFormatter.ISO_OFFSET_DATE_TIME.format(inst.atOffset(ZoneOffset.UTC))
      case dt: DecimalType =>
        row.getDecimal(i, dt.precision, dt.scale).toString()
      case other =>
        // TODO arrays, maps
        sys.error(s"Can't serialize $other values")
    }
  }
}
