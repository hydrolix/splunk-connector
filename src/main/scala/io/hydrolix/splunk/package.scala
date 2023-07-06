package io.hydrolix

import io.hydrolix.spark.model.JSON

import com.google.common.io.ByteStreams
import com.google.common.primitives.Bytes
import org.slf4j.LoggerFactory

import java.io.{ByteArrayOutputStream, InputStream, ObjectOutputStream, OutputStream, PushbackInputStream}
import java.util.Base64
import java.util.zip.GZIPOutputStream
import scala.concurrent.duration.Duration
import scala.concurrent.{Await, Future}
import scala.concurrent.ExecutionContext.Implicits.global

package object splunk {
  private val logger = LoggerFactory.getLogger(getClass)

  val partitionMetaColumns = List("hdx_db", "hdx_table", "hdx_path", "hdx_schema", "hdx_pushed")

  private val chunkedHeaderR = """^chunked 1.0,(\d+),(\d+)$""".r
  private val tableArgR = """^table\s*=\s*(.*?)\.(.*?)$""".r

  def getDbTableArg(meta: ChunkedRequestMetadata): (String, String) = {
    meta.searchInfo.args.collectFirst {
      case tableArgR(db, table) => (db.trim.toLowerCase, table.trim.toLowerCase)
    }.getOrElse(sys.error("Couldn't find table=db.table argument!"))
  }

  def readInitialChunk(stdin: InputStream): ChunkedRequestMetadata = {
    readChunk(stdin) match {
      case (Some(meta), _) =>
        meta
      case (None, _) =>
        sys.error("Couldn't read initial chunk header!")
    }
  }

  /**
   * Read a chunk from an input stream. stdin must be positioned before a chunk header line; does not close the stream
   *
   * @param stdin the input stream to read from. Must be positioned before a chunk header line.
   * @return (metadata, Option[(dataLength, dataInputStream)]
   */
  def readChunk(stdin: InputStream): (Option[ChunkedRequestMetadata], Option[(Int, InputStream)]) = {
    val readF = Future {
      val pb = new PushbackInputStream(stdin, 128)

      val headerBuf = Array.ofDim[Byte](128)

      stdin.read(headerBuf) match {
        case -1 =>
          (None, None)
        case len =>
          val pos = Bytes.indexOf(headerBuf, '\n'.toByte)
          if (pos == -1) sys.error("Expected header line within first 128 bytes")
          val headerLine = new String(headerBuf, 0, pos)

          pb.unread(headerBuf, pos + 1, len - pos - 1)

          headerLine match {
            case chunkedHeaderR(mlen, dlen) =>
              val mbuf = Array.ofDim[Byte](mlen.toInt)

              val metaLen = pb.read(mbuf)
              val dataLen = dlen.toInt

              val meta = if (metaLen > 0) JSON.objectMapper.readValue[ChunkedRequestMetadata](mbuf) else sys.error(s"Expected metadata but size was $mlen")
              val data = if (dataLen > 0) Some(dataLen -> pb) else None

              Some(meta) -> data
            case other =>
              sys.error(s"Malformed header line: `$other` (expected `chunked 1.0,<metaLen>,<dataLen>`")
          }
      }
    }

    Await.result(readF, Duration.Inf)
  }

  /**
   * Write a chunk to an output stream
   *
   * @param out      output stream to write to
   * @param metadata metadata bytes
   * @param data     Some((dataLength, dataInputStream)) if any data is to be written, or None if not
   */
  def writeChunk(out: OutputStream, metadata: Array[Byte], data: Option[(Int, InputStream)]): Unit = {
    val dataLen = data.map(_._1).getOrElse(0)

    out.write(s"chunked 1.0,${metadata.length},$dataLen\n".getBytes("UTF-8"))
    out.write(metadata)

    data.foreach { case (_, stream) =>
      ByteStreams.copy(stream, out)
    }
  }

  def serialize(obj: Serializable): Array[Byte] = {
    val baos = new ByteArrayOutputStream(8192)
    val out = new ObjectOutputStream(baos)
    out.writeObject(obj)
    out.close()
    baos.toByteArray
  }

  def compress(bytes: Array[Byte]): String = {
    val baos = new ByteArrayOutputStream(8192)
    val out = new GZIPOutputStream(baos)
    out.write(bytes)
    out.close()
    Base64.getEncoder.encodeToString(baos.toByteArray)
  }
}
