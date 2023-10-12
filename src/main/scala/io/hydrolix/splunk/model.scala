package io.hydrolix.splunk

import java.net.URI
import java.time.Instant
import java.util
import java.util.UUID

import com.fasterxml.jackson.annotation.JsonProperty
import com.fasterxml.jackson.core.{JsonGenerator, JsonParser}
import com.fasterxml.jackson.databind.PropertyNamingStrategies.SnakeCaseStrategy
import com.fasterxml.jackson.databind.annotation.{JsonDeserialize, JsonNaming, JsonSerialize}
import com.fasterxml.jackson.databind.{DeserializationContext, JsonDeserializer, JsonSerializer, SerializerProvider}

import io.hydrolix.connectors.types.StructType
import io.hydrolix.connectors.{HdxConnectionInfo, HdxStorageSettings, HdxValueType}
import io.hydrolix.splunk

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

sealed trait KVStoreAccess {
  val uri: URI
  def authHeaderValue: String
}

case class KVStorePassword(uri: URI, login: String, password: String) extends KVStoreAccess {
  override val authHeaderValue = {
    val cred = util.Base64.getEncoder.encodeToString(s"$login:$password".getBytes("UTF-8"))
    s"Basic $cred"
  }
}

case class KVStoreSessionKey(uri: URI, sessionKey: String) extends KVStoreAccess {
  override val authHeaderValue = s"Splunk $sessionKey"
}

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
) {
  val connectionInfo =
    HdxConnectionInfo(
      jdbcUrl,
      username,
      password,
      apiUrl,
      None,
      cloudCred1,
      cloudCred2,
      None
    )
}

/**
 * TODO set up a saved search to delete old plans
 *
 * @param sid             the Splunk search ID, cleaned so `remote_<host.name>_<timestamp>` leaves just the timestamp
 * @param user            the user who owns this record; will be set by Splunk
 * @param timestamp       system time (in millis) on the planner when this plan was created; can be used for future garbage
 *                        collection
 * @param plannerId       ID of the node that had planner responsibility for this query
 * @param workerIds       IDs of the nodes that the planner assigned at least one scan job to for this query
 * @param db              name of the Hydrolix database
 * @param table           name of the Hydrolix table
 * @param primaryKeyField name of the primary timestamp field in the Hydrolix table
 * @param primaryKeyType  value type of the primary timestamp field in the Hydrolix table
 * @param cols            Spark struct of the columns this query will need to read
 * @param storages        Hydrolix storage metadata for accessing cloud storage
 * @param minTimestamp    lower time bound for this query
 * @param maxTimestamp    upper time bound for this query
 * @param otherTerms      Map[fieldName, value] of additional search terms for this query. Always equality, always AND.
 * @param predicatesBlob  base64(gzip(serialize(_))) of predicates that were pushed down
 */
@JsonNaming(classOf[SnakeCaseStrategy])
case class QueryPlan(
  @JsonProperty("_key")   sid: String,
  @JsonProperty("_user") user: Option[String],
                    timestamp: Long,
                    plannerId: UUID,
                    workerIds: List[UUID],
                           db: String,
                        table: String,
              primaryKeyField: String,
               primaryKeyType: HdxValueType,
  @JsonSerialize(using=classOf[StructTypeSerializer])
  @JsonDeserialize(using=classOf[StructTypeDeserializer])
                         cols: StructType,
                     storages: Map[UUID, HdxStorageSettings],
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
             assignedWorkerId: UUID,
                      claimed: Boolean,
               partitionPaths: List[String],
                   storageIds: List[UUID])

final class StructTypeSerializer extends JsonSerializer[StructType] {
  override def serialize(value: StructType, gen: JsonGenerator, serializers: SerializerProvider): Unit = {
    gen.writeString(compress(splunk.serialize(value)))
  }
}

final class StructTypeDeserializer extends JsonDeserializer[StructType] {
  override def deserialize(p: JsonParser, ctxt: DeserializationContext): StructType = {
    splunk.deserialize(decompress(p.getValueAsString))
  }
}