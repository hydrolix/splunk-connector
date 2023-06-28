package io.hydrolix.splunk

import io.hydrolix.spark.model.JSON

import com.fasterxml.jackson.annotation.JsonProperty
import com.fasterxml.jackson.databind.PropertyNamingStrategies.SnakeCaseStrategy
import com.fasterxml.jackson.databind.annotation.JsonNaming
import org.slf4j.LoggerFactory

import java.io.{BufferedReader, InputStreamReader}
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration.Duration
import scala.concurrent.{Await, Future}

@JsonNaming(classOf[SnakeCaseStrategy])
case class ChunkedMetadata(       action: String,
                                 preview: Boolean,
             streamingCommandWillRestart: Boolean,
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


object HdxPlanCommand extends App {
  private val logger = LoggerFactory.getLogger(getClass)

  private val chunkedHeaderR = """chunked 1.0,(\d+),(\d+)""".r

  /**
   * Reader must be positioned before a chunk header line
   */
  private def readChunk(stdin: BufferedReader): (Option[ChunkedMetadata], Option[String]) = {
    var meta: Option[ChunkedMetadata] = None
    var data: Option[String] = None

    val readF = Future {
      stdin.readLine() match {
        case chunkedHeaderR(mlen, dlen) =>
          val mbuf = Array.ofDim[Char](mlen.toInt)
          val dbuf = Array.ofDim[Char](dlen.toInt)
          val metaLen = stdin.read(mbuf)
          val dataLen = stdin.read(dbuf)

          meta = if (metaLen > 0) Some(JSON.objectMapper.readValue[ChunkedMetadata](new String(mbuf, 0, metaLen))) else sys.error(s"Expected metadata but size was $mlen")
          data = if (dataLen > 0) Some(new String(dbuf, 0, dataLen)) else None
        case null =>
          // OK, stream is done
          meta = None
          data = None
        case other =>
          System.err.println(s"Got unexpected header line: $other")
          meta = None
          data = None
      }
    }

    Await.result(readF, Duration.Inf)

    meta -> data
  }

  def writeOutput(metadata: String, data: String): Unit = {
    System.out.println(s"chunked 1.0,${metadata.length},${data.length}")
    System.out.println(metadata)
    System.out.println(data)
  }

  val stdin = new BufferedReader(new InputStreamReader(System.in, "UTF-8"))

  val (getInfoMeta, getInfoData) = readChunk(stdin)
  logger.info(s"Got getinfo metadata: $getInfoMeta")
  logger.info(s"Got getinfo data: $getInfoData")

  writeOutput("""{"type": "streaming", "generating": true}""","")

  val (execMeta, execData) = readChunk(stdin)

  logger.info(s"Got exec metadata: $execMeta")
  logger.info(s"Got exec data: $execData")

  writeOutput("""{"finished":true}""", "")

  stdin.close()
}
