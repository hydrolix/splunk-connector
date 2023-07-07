package io.hydrolix.splunk

import io.hydrolix.spark.model.JSON

import com.github.tototoshi.csv.CSVWriter
import org.apache.spark.sql.catalyst.util.DateTimeUtils
import org.slf4j.LoggerFactory

import java.io.{ByteArrayInputStream, ByteArrayOutputStream}
import java.net.URI

object HdxPlanCommand {
  private val logger = LoggerFactory.getLogger(getClass)

  def main(args: Array[String]): Unit = {
    val getInfoMeta = readInitialChunk(System.in)

    logger.info(s"PLAN getinfo metadata: $getInfoMeta")

    val resp = GetInfoResponseMeta(
      CommandType.events,
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
      case (Some(execMeta), execData) =>
        logger.info(s"PLAN execute metadata: $execMeta")
        logger.info(s"PLAN execute data: $execData")

        val info = Config.loadWithSessionKey(new URI(getInfoMeta.searchInfo.splunkdUri), getInfoMeta.searchInfo.sessionKey)
        logger.info(s"PLAN config loaded: ${info.toString}")

        val (dbName, tableName) = getDbTableArg(getInfoMeta)

        val minTimestamp = DateTimeUtils.microsToInstant((getInfoMeta.searchInfo.earliestTime * 1000000).toLong)
        val maxTimestamp = DateTimeUtils.microsToInstant((getInfoMeta.searchInfo.latestTime * 1000000).toLong)

        val cat = tableCatalog(info)
        val table = hdxTable(cat, dbName, tableName)
        val cols = getRequestedCols(getInfoMeta, table)
        val partitions = planPartitions(table, cols, minTimestamp, maxTimestamp, info)

        val out = new ByteArrayOutputStream(16384)
        val writer = CSVWriter.open(out)
        writer.writeRow(partitionMetaColumns)

        for (partition <- partitions) {
          writer.writeRow(List(
            partition.db,
            partition.table,
            partition.path,
            JSON.objectMapper.writeValueAsString(partition.hdxCols),
            compress(serialize(partition.pushed))
          ))
        }

        out.close()
        val outBytes = out.toByteArray

        val resp2 = ExecuteResponseMeta(true)
        writeChunk(System.out, JSON.objectMapper.writeValueAsBytes(resp2), Some((outBytes.length, new ByteArrayInputStream(outBytes))))

        System.in.close()

      case (None, _) =>
        // TODO do we need to send or expect a finished=true?
        sys.exit(0)
    }
  }
}
