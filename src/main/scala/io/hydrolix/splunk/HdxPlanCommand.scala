package io.hydrolix.splunk

import io.hydrolix.spark.model.JSON

import com.fasterxml.jackson.annotation.JsonProperty
import com.fasterxml.jackson.databind.PropertyNamingStrategies.SnakeCaseStrategy
import com.fasterxml.jackson.databind.annotation.JsonNaming
import org.slf4j.LoggerFactory

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
                                earliestTime: Int,
                                  latestTime: Int)

@JsonNaming(classOf[SnakeCaseStrategy])
case class GetInfoResponseMeta(   `type`: CommandType,
                              generating: Boolean,
                          requiredFields: List[String],
 @JsonProperty("maxwait")        maxWait: Option[Int],
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

object HdxPlanCommand extends App {
  private val logger = LoggerFactory.getLogger(getClass)

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

  val (execMeta, execData) = readChunk(System.in)

  logger.info(s"Got exec metadata: $execMeta")
  logger.info(s"Got exec data: $execData")

  val resp2 = ExecuteResponseMeta(true)
  writeChunk(System.out, JSON.objectMapper.writeValueAsBytes(resp2), None)

  System.in.close()
}
