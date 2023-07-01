package io.hydrolix.splunk

import io.hydrolix.spark.connector.{HdxScanBuilder, HdxTable, HdxTableCatalog}
import io.hydrolix.spark.model.{HdxConnectionInfo, JSON}

import com.fasterxml.jackson.annotation.JsonProperty
import com.fasterxml.jackson.databind.PropertyNamingStrategies.SnakeCaseStrategy
import com.fasterxml.jackson.databind.annotation.JsonNaming
import org.apache.spark.sql.HdxPushdown.GetField
import org.apache.spark.sql.connector.catalog.Identifier
import org.apache.spark.sql.connector.expressions.LiteralValue
import org.apache.spark.sql.connector.expressions.filter.{And, Predicate}
import org.apache.spark.sql.types.DataTypes
import org.apache.spark.sql.util.CaseInsensitiveStringMap
import org.slf4j.LoggerFactory

import java.net.URI
import scala.jdk.CollectionConverters._

@JsonNaming(classOf[SnakeCaseStrategy])
case class ChunkedRequestMetadata(action: String,
                                 preview: Boolean,
             streamingCommandWillRestart: Boolean,
                                finished: Boolean,
  @JsonProperty("searchinfo") searchInfo: SearchInfo)

@JsonNaming(classOf[SnakeCaseStrategy])
case class SearchInfo(                  args: List[String],
                                     rawArgs: List[String],
                                 dispatchDir: String,
                                         sid: String,
                                         app: String,
                                       owner: String,
                                    username: String,
                                  sessionKey: String,
                                  splunkdUri: String,
                               splunkVersion: String,
                                      search: String,
                                     command: String,
@JsonProperty("maxresultrows") maxResultRows: Int,
                                earliestTime: BigDecimal,
                                  latestTime: BigDecimal)

@JsonNaming(classOf[SnakeCaseStrategy])
case class GetInfoResponseMeta(`type`: CommandType,
                           generating: Boolean,
                       requiredFields: List[String],
  @JsonProperty("maxwait")    maxWait: Option[Int],
                       streamingPreop: String,
                             finished: Boolean,
                                error: String,
                            inspector: InspectorMessages)

case class InspectorMessages(messages: List[(String, String)])

/**
 * TODO there should definitely be more fields here...
 */
@JsonNaming(classOf[SnakeCaseStrategy])
case class ExecuteResponseMeta(finished: Boolean)

object HdxPlanCommand {
  private val logger = LoggerFactory.getLogger(getClass)

  def main(args: Array[String]): Unit = {
    val (getInfoMeta, getInfoData) = readChunk(System.in)
    logger.info(s"Got getinfo metadata: $getInfoMeta")
    logger.info(s"Got getinfo data: $getInfoData")

    val resp = GetInfoResponseMeta(
      CommandType.streaming,
      true,
      Nil,
      Some(10),
      "",
      false,
      "",
      InspectorMessages(Nil)
    )

    writeChunk(System.out, JSON.objectMapper.writeValueAsBytes(resp), None)

    val info = Config.loadSessionKey(getInfoMeta.searchInfo.sessionKey)
    logger.info(info.toString)

    val opts = new CaseInsensitiveStringMap((Map(
      HdxConnectionInfo.OPT_API_URL -> info.apiUrl.toString,
      HdxConnectionInfo.OPT_JDBC_URL -> info.jdbcUrl,
      HdxConnectionInfo.OPT_USERNAME -> info.user,
      HdxConnectionInfo.OPT_PASSWORD -> info.password,
      HdxConnectionInfo.OPT_CLOUD_CRED_1 -> info.cloudCred1)
      ++ info.cloudCred2.map(HdxConnectionInfo.OPT_CLOUD_CRED_2 -> _).toMap
      ).asJava)

    val cat = new HdxTableCatalog()
    cat.initialize("hydrolix", opts)
    val tableName = getInfoMeta.searchInfo.args.find(_.startsWith("table=")).map(_.drop(6).trim).getOrElse(sys.error("table argument is required!"))
    val bits = tableName.split('.')
    if (bits.length != 2) sys.error("Table name must be in `db.table` format")
    val table = cat.loadTable(Identifier.of(Array(bits(0)), bits(1))).asInstanceOf[HdxTable]
    val sb = new HdxScanBuilder(info, table.storage, table)

    val minTimeMicros = (getInfoMeta.searchInfo.earliestTime / 1000000).toLongExact
    val maxTimeMicros = (getInfoMeta.searchInfo.latestTime / 1000000).toLongExact
    sb.pushPredicates(Array(new And(
      new Predicate(">=", Array(GetField(table.primaryKeyField), LiteralValue(minTimeMicros, DataTypes.LongType))),
      new Predicate("<=", Array(GetField(table.primaryKeyField), LiteralValue(maxTimeMicros, DataTypes.LongType)))
    )))

    val scan = sb.build()
    val batch = scan.toBatch
    val partitions = batch.planInputPartitions()
    // TODO write the partition metadata to output


    val (execMeta, execData) = readChunk(System.in)

    logger.info(s"Got exec metadata: $execMeta")
    logger.info(s"Got exec data: $execData")

    val resp2 = ExecuteResponseMeta(true)
    writeChunk(System.out, JSON.objectMapper.writeValueAsBytes(resp2), None)

    System.in.close()
  }
}

@JsonNaming(classOf[SnakeCaseStrategy])
case class HdxConfig(
  @JsonProperty("_key")               _key: String,
  @JsonProperty("_user")             _user: String,
                                   jdbcUrl: String,
                                    apiUrl: URI,
                                  username: String,
                                  password: String,
  @JsonProperty("cloud_cred_1") cloudCred1: String,
  @JsonProperty("cloud_cred_2") cloudCred2: Option[String])
