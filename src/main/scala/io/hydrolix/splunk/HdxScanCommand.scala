package io.hydrolix.splunk

import io.hydrolix.spark.model.JSON

import org.slf4j.LoggerFactory

object HdxScanCommand {
  private val logger = LoggerFactory.getLogger(getClass)

  def main(args: Array[String]): Unit = {
    val getInfoMeta = readInitialChunk(System.in)
    logger.info(s"SCAN getinfo metadata: $getInfoMeta")

    val (db, table) = getDbTableArg(getInfoMeta)

    val resp = GetInfoResponseMeta(
      CommandType.streaming,
      generating = false,
      partitionMetaColumns,
      Some(600),
      s"|hdxplan table=$db.$table",
      finished = false,
      "",
      InspectorMessages(Nil)
    )

    writeChunk(System.out, JSON.objectMapper.writeValueAsBytes(resp), None)

    readChunk(System.in) match {
      case (None, _) => sys.exit(0)
      case (Some(execMeta), execData) =>
        sys.error("TODO handle execute phase of hdxscan")
    }
  }
}
