package io.hydrolix.splunk

import io.hydrolix.spark.connector.{HdxScanBuilder, HdxScanPartition, HdxTable, HdxTableCatalog}
import io.hydrolix.spark.model.{HdxConnectionInfo, JSON}

import com.github.tototoshi.csv.CSVWriter
import org.apache.spark.sql.HdxPushdown.{GetField, Literal}
import org.apache.spark.sql.catalyst.util.DateTimeUtils
import org.apache.spark.sql.connector.catalog.Identifier
import org.apache.spark.sql.connector.expressions.filter.{And, Predicate}
import org.apache.spark.sql.types.{StructField, StructType}
import org.apache.spark.sql.util.CaseInsensitiveStringMap
import org.slf4j.LoggerFactory

import java.io.{ByteArrayInputStream, ByteArrayOutputStream}
import java.net.URI
import scala.jdk.CollectionConverters._

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

        val info = Config.loadSessionKey(new URI(getInfoMeta.searchInfo.splunkdUri), getInfoMeta.searchInfo.sessionKey)
        logger.info(info.toString)

        val (db: String, table: String) = getDbTableArg(getInfoMeta)

        val partitions = planPartitions(db, table, getInfoMeta.searchInfo.earliestTime, getInfoMeta.searchInfo.latestTime, info)

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

  private def planPartitions(dbName: String,
                          tableName: String,
                           earliest: BigDecimal,
                             latest: BigDecimal,
                               info: HdxConnectionInfo)
                                   : List[HdxScanPartition] =
  {
    val opts = new CaseInsensitiveStringMap(info.asMap.asJava)

    val catalog = new HdxTableCatalog()
    catalog.initialize("hydrolix", opts)
    val table = catalog.loadTable(Identifier.of(Array(dbName), tableName)).asInstanceOf[HdxTable]
    val sb = new HdxScanBuilder(info, table.storage, table)
    sb.pruneColumns(StructType(table.hdxCols.values.toList.map { hcol =>
      StructField(hcol.name, hcol.sparkType, hcol.nullable)
    }))

    val minTimestamp = DateTimeUtils.microsToInstant((earliest * 1000000).toLong)
    val maxTimestamp = DateTimeUtils.microsToInstant((latest * 1000000).toLong)
    logger.info(s"minTimestamp: $minTimestamp")
    logger.info(s"maxTimestamp: $maxTimestamp")

    sb.pushPredicates(Array(new And(
      new Predicate(">=", Array(GetField(table.primaryKeyField), Literal(minTimestamp))),
      new Predicate("<=", Array(GetField(table.primaryKeyField), Literal(maxTimestamp)))
    )))

    val scan = sb.build()
    val batch = scan.toBatch

    batch.planInputPartitions().toList.map(_.asInstanceOf[HdxScanPartition])
  }
}
