package io.hydrolix

import io.hydrolix.spark.connector.{HdxScanBuilder, HdxScanPartition, HdxTable, HdxTableCatalog}
import io.hydrolix.spark.model.{HdxConnectionInfo, JSON}

import com.google.common.io.ByteStreams
import com.google.common.primitives.Bytes
import org.apache.spark.sql.connector.catalog.Identifier
import org.apache.spark.sql.connector.expressions.filter.Predicate
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.util.CaseInsensitiveStringMap
import org.slf4j.LoggerFactory

import java.io.{ByteArrayInputStream, ByteArrayOutputStream, InputStream, ObjectInputStream, ObjectOutputStream, OutputStream, PushbackInputStream}
import java.util.Base64
import java.util.zip.{GZIPInputStream, GZIPOutputStream}
import scala.collection.mutable
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration.Duration
import scala.concurrent.{Await, Future}
import scala.jdk.CollectionConverters.MapHasAsJava
import scala.util.Using

package object splunk {
  private val logger = LoggerFactory.getLogger(getClass)

  private val chunkedHeaderR = """^chunked 1.0,(\d+),(\d+)$""".r
  private val tableArgR = """^table\s*=\s*(.*?)\.(.*?)$""".r
  private val fieldsArgR = """^fields\s*=\s*(.*?)$""".r
  private val otherArgR = """^(.*?)\s*=\s*(.*?)$""".r

  def getDbTableArg(args: List[String]): (String, String) = {
    args.collectFirst {
      case tableArgR(db, table) => (db.trim.toLowerCase, table.trim.toLowerCase)
    }.getOrElse(sys.error("Couldn't find table=db.table argument!"))
  }

  private def getFieldsArg(args: List[String]): List[String] = {
    args.collectFirst {
      case fieldsArgR(s) => s.split(",\\s*").toList
    }.getOrElse(Nil)
  }

  def getOtherTerms(args: List[String]): mutable.LinkedHashMap[String, String] = {
    mutable.LinkedHashMap(args.collect {
      case otherArgR(name, value) if name != "fields" && name != "table" => name -> value
    }: _*)
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
    Using.Manager { use =>
      val baos = use(new ByteArrayOutputStream(8192))
      val out = use(new ObjectOutputStream(baos))
      out.writeObject(obj)
      out.close()
      baos.toByteArray
    }.get
  }

  def compress(bytes: Array[Byte]): String = {
    Using.Manager { use =>
      val baos = use(new ByteArrayOutputStream(8192))
      val out = use(new GZIPOutputStream(baos))

      out.write(bytes)
      out.close()
      Base64.getEncoder.encodeToString(baos.toByteArray)
    }.get
  }

  def decompress(base64: String): Array[Byte] = {
    Using.Manager { use =>
      val bais = use(new ByteArrayInputStream(Base64.getDecoder.decode(base64)))
      val is = use(new GZIPInputStream(bais))
      ByteStreams.toByteArray(is)
    }.get
  }

  def deserialize[A <: Serializable](bytes: Array[Byte]): A = {
    Using.Manager { use =>
      val bais = use(new ByteArrayInputStream(bytes))
      val ois = use(new ObjectInputStream(bais))
      ois.readObject().asInstanceOf[A]
    }.get
  }

  def tableCatalog(info: HdxConnectionInfo): HdxTableCatalog = {
    val opts = new CaseInsensitiveStringMap(info.asMap.asJava)

    val catalog = new HdxTableCatalog()
    catalog.initialize("hydrolix", opts)
    catalog
  }

  def hdxTable(cat: HdxTableCatalog, dbName: String, tableName: String): HdxTable = {
    cat.loadTable(Identifier.of(Array(dbName), tableName)).asInstanceOf[HdxTable]
  }

  def getRequestedCols(args: List[String], table: HdxTable): StructType = {
    val requestedFields = getFieldsArg(args)
    val otherTerms = getOtherTerms(args)

    val fields = (List(table.primaryKeyField) ++ requestedFields ++ otherTerms.keys).distinct

    if (fields.isEmpty) {
      table.schema
    } else {
      val schemaFields = table.schema.map { field => field.name -> field }.toMap
      StructType(fields.flatMap { name =>
        val mf = schemaFields.get(name)
        if (mf.isEmpty) logger.warn(s"Requested field $name not found in table schema (${table.schema})")
        mf
      })
    }
  }

  def planPartitions(table: HdxTable,
                      cols: StructType,
                predicates: List[Predicate],
                      info: HdxConnectionInfo)
                          : List[HdxScanPartition] =
  {
    val sb = new HdxScanBuilder(info, table)
    sb.pruneColumns(cols)
    sb.pushPredicates(predicates.toArray)

    val scan = sb.build()
    val batch = scan.toBatch

    batch.planInputPartitions().toList.map(_.asInstanceOf[HdxScanPartition])
  }

}
