package io.hydrolix.splunk

import io.hydrolix.spark.model.{HdxStorageSettings, HdxValueType}

import com.fasterxml.jackson.annotation.JsonProperty
import com.fasterxml.jackson.databind.PropertyNamingStrategies.SnakeCaseStrategy
import com.fasterxml.jackson.databind.annotation.JsonNaming
import org.apache.spark.sql.types.StructType

import java.net.URI
import java.time.Instant
import java.util.UUID

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


/** JSON object for KVstore version of HdxConnectionInfo */
@JsonNaming(classOf[SnakeCaseStrategy])
case class HdxConfig(
  @JsonProperty("_key")               _key: String,
  @JsonProperty("_user")             _user: String,
                                   jdbcUrl: String,
                                    apiUrl: URI,
                                  username: String,
                                  password: String,
  @JsonProperty("cloud_cred_1") cloudCred1: String,
  @JsonProperty("cloud_cred_2") cloudCred2: Option[String],
                          zookeeperServers: List[String]
)

/**
 * TODO set up a saved search to delete old plans
 */
@JsonNaming(classOf[SnakeCaseStrategy])
case class QueryPlan(
  @JsonProperty("_key")   sid: String,
  @JsonProperty("_user") user: Option[String],
                    timestamp: Long,
                    plannerId: UUID,
                           db: String,
                        table: String,
              primaryKeyField: String,
               primaryKeyType: HdxValueType,
                         cols: StructType,
                      storage: HdxStorageSettings,
                 minTimestamp: Instant,
                 maxTimestamp: Instant,
                   otherTerms: Map[String, String],
               predicatesBlob: String, // base64(gzip(serialized Array[Predicate]))
)

@JsonNaming(classOf[SnakeCaseStrategy])
case class ScanJob(
  @JsonProperty("_key")   key: String, // sid + _ + workerId
  @JsonProperty("_user") user: Option[String],
                    timestamp: Long,
                          sid: String,
                     workerId: UUID,
               partitionPaths: List[String])