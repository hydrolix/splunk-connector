package io.hydrolix.splunk

import io.hydrolix.spark.connector.{HdxPartitionReader, HdxScanPartition}
import io.hydrolix.spark.model.{HdxColumnDatatype, HdxColumnInfo, HdxConnectionInfo, HdxValueType, JSON}

import com.github.tototoshi.csv.CSVWriter
import org.apache.curator.framework.CuratorFrameworkFactory
import org.apache.curator.framework.recipes.leader.{LeaderLatch, LeaderLatchListener}
import org.apache.curator.retry.ExponentialBackoffRetry
import org.apache.spark.sql.HdxPushdown.{GetField, Literal}
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.util.DateTimeUtils
import org.apache.spark.sql.connector.expressions.filter.{And, Predicate}
import org.apache.spark.sql.types._
import org.slf4j.{Logger, LoggerFactory}

import java.io.{File, FileInputStream, FileOutputStream}
import java.net.URI
import java.time.ZoneOffset
import java.time.format.DateTimeFormatter
import java.util.UUID
import java.util.concurrent.CountDownLatch
import scala.collection.mutable
import scala.collection.mutable.ListBuffer
import scala.jdk.CollectionConverters._

object HdxQueryCommand {
  private val logger = LoggerFactory.getLogger(getClass)

  private val MaxPlanAttempts = 10
  private val PlanAttemptWait = 1000
  private val MaxScanAttempts = 10
  private val ScanAttemptWait = 1000

  private val remoteSidR = """^remote_.*?_([\d.]+)$""".r
  private val cleanSidR = """^([\d.]+)$""".r

  def main(args: Array[String]): Unit = {
    val getInfoMeta = readInitialChunk(System.in)
    logger.info(s"INIT: getinfo metadata: $getInfoMeta")

    val kvStoreAccess = if (args.length >= 3) {
      logger.info("INIT: Accessing KVStore with basic auth")
      KVStorePassword(new URI(args(0)), args(1), args(2))
    } else {
      logger.info("INIT: Accessing KVStore with session key")
      KVStoreSessionKey(new URI(getInfoMeta.searchInfo.splunkdUri), getInfoMeta.searchInfo.sessionKey)
    }

    val resp = GetInfoResponseMeta(
      CommandType.streaming,
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
      case (None, _) => sys.exit(0)
      case (Some(execMeta), execData) =>
        val hc = Config.load(kvStoreAccess)

        val nodeId = UUID.randomUUID()

        logger.info(s"INIT-$nodeId: config loaded: ${hc.toString}")

        val curatorClient = CuratorFrameworkFactory.newClient(
          hc.zookeeperServers.mkString(","),
          new ExponentialBackoffRetry(500, 10) // TODO make the retry settings configurable
        )

        curatorClient.start()

        val info = HdxConnectionInfo(
          hc.jdbcUrl,
          hc.username,
          hc.password,
          hc.apiUrl,
          None,
          hc.cloudCred1,
          hc.cloudCred2
        )

        val now = System.currentTimeMillis()

        val sid = getInfoMeta.searchInfo.sid match {
          case remoteSidR(s) => s
          case cleanSidR(s) => s
          case other => sys.error(s"Couldn't parse clean sid from $other")
        }

        val ll = new LeaderLatch(curatorClient, s"/search/$sid/planner", nodeId.toString)
        logger.info(s"INIT-$nodeId: Starting LeaderLatch")
        ll.start()
        logger.info(s"INIT-$nodeId: LeaderLatch started")

        // Make sure this command's main thread waits for either callback to finish
        val latch = new CountDownLatch(1)

        ll.addListener(new LeaderLatchListener {
          override def isLeader(): Unit = {
            logger.info(s"PLANNER-$nodeId: I'm the planner")

            val (plan, partitionPaths) = {
              val (dbName, tableName) = getDbTableArg(getInfoMeta)

              val cat = tableCatalog(info)

              val table = hdxTable(cat, dbName, tableName)

              val cols = getRequestedCols(getInfoMeta, table)
              logger.info(s"PLANNER-$nodeId: Requested columns: $cols")

              val minTimestamp = DateTimeUtils.microsToInstant((getInfoMeta.searchInfo.earliestTime * 1000000).toLong)
              val maxTimestamp = DateTimeUtils.microsToInstant((getInfoMeta.searchInfo.latestTime * 1000000).toLong)

              val otherTerms = getOtherTerms(getInfoMeta).toMap

              val predicates = List(new And(
                new Predicate(">=", Array(GetField(table.primaryKeyField), Literal(minTimestamp))),
                new Predicate("<=", Array(GetField(table.primaryKeyField), Literal(maxTimestamp))),
              )) ++ otherTerms.map {
                case (k, v) => new Predicate("=", Array(GetField(k), Literal(v)))
              }

              val partitions = planPartitions(table, cols, minTimestamp, maxTimestamp, predicates, info)

              val blob = compress(serialize(predicates))

              (
                QueryPlan(
                  sid,
                  None,
                  now,
                  nodeId,
                  dbName,
                  tableName,
                  table.primaryKeyField,
                  table.hdxCols(table.primaryKeyField).hdxType.`type`,
                  cols,
                  table.storage,
                  minTimestamp,
                  maxTimestamp,
                  otherTerms,
                  blob
                ),
                partitions.map(_.path)
              )
            }

            Config.writePlan(kvStoreAccess, plan)

            val workers = ll.getParticipants.asScala.toVector
            val numWorkers = workers.size
            logger.info(s"PLANNER-$nodeId: Assigning ${partitionPaths.size} partitions to $numWorkers workers")
            val partitionsPerWorker = mutable.Map[String, mutable.ListBuffer[String]]().withDefaultValue(ListBuffer())

            // Allocate every partition that needs to be scanned to one of the workers
            for ((part, i) <- partitionPaths.zipWithIndex) {
              val worker = workers(i % numWorkers)
              partitionsPerWorker(worker.getId) += part
            }

            // Write out the per-worker partition lists under the worker IDs
            for ((workerId, partitions) <- partitionsPerWorker) {
              logger.info(s"PLANNER-$nodeId: Worker $workerId should scan ${partitions.size} partitions")
              Config.writeScanJob(
                kvStoreAccess,
                ScanJob(s"${sid}_$workerId", None, now, sid, UUID.fromString(workerId), partitions.toList)
              )
            }

            // Transition to worker mode

            logger.info(s"PLANNER-$nodeId: I'm a worker now")

            val scanJob = retry(
              logger, s"WORKER-$nodeId: Waiting for scan job",
              MaxScanAttempts, ScanAttemptWait,
              Config.readScanJob(kvStoreAccess, sid, nodeId)
            ).getOrElse(sys.error("Couldn't get scan job"))

            scan(nodeId, info, plan, scanJob.partitionPaths)

            latch.countDown()
          }

          override def notLeader(): Unit = {
            logger.info(s"WORKER-$nodeId: I'm a worker")

            val plan = retry(
              logger, s"WORKER-$nodeId: Waiting for query plan",
              MaxPlanAttempts, PlanAttemptWait,
              Config.readPlan(kvStoreAccess, sid)
            ).getOrElse(sys.error("Couldn't get query plan"))

            val scanJob = retry(
              logger, s"WORKER-$nodeId: Waiting for scan job",
              MaxScanAttempts, ScanAttemptWait,
              Config.readScanJob(kvStoreAccess, sid, nodeId)
            ).getOrElse(sys.error("Couldn't get scan job"))

            scan(nodeId, info, plan, scanJob.partitionPaths)

            latch.countDown()
          }
        })

        latch.await()
    }
  }

  private def retry[A](logger: Logger, what: String, maxRetries: Int, delay: Int, f: => Option[A]): Option[A] = {
    var count = 0
    while (count < maxRetries) {
      count += 1
      f match {
        case Some(value) => return Some(value)
        case None if count <= maxRetries =>
          logger.info(s"$what attempt #$count/$maxRetries")
          Thread.sleep(delay)
        case None => return None
      }
    }
    None
  }

  private def scan(workerId: UUID, info: HdxConnectionInfo, qp: QueryPlan, partitionPaths: List[String]): Unit = {
    // TODO do output in 50k chunks over multiple iterations, not just a single spew
    // TODO do output in 50k chunks over multiple iterations, not just a single spew
    // TODO do output in 50k chunks over multiple iterations, not just a single spew
    // TODO do output in 50k chunks over multiple iterations, not just a single spew

    val tmp = File.createTempFile("hdx_output", ".csv")
    tmp.deleteOnExit()
    val writer = CSVWriter.open(new FileOutputStream(tmp))
    writer.writeRow(qp.cols.map(_.name))

    val preds = deserialize[List[Predicate]](decompress(qp.predicatesBlob))

    var count = 0
    var written = 0
    for (path <- partitionPaths) {
      val timestampPos = qp.cols.fieldIndex(qp.primaryKeyField)
      val otherArgPoss = qp.otherTerms.map {
        case (name, value) =>
          val pos = qp.cols.fieldIndex(name)
          val typ = qp.cols.fields(pos).dataType
          if (typ != DataTypes.StringType) sys.error(s"Can't search for $name of type $typ (only Strings)")
          pos -> value
      }

      val hdxCols = qp.cols.fields.map { sf =>
        val hdxType = spark2Hdx(sf.name, sf.dataType, qp.primaryKeyField, qp.primaryKeyType)
        sf.name -> HdxColumnInfo(sf.name, hdxType, nullable = true, sf.dataType, 1)
      }.toMap

      val pr = new HdxPartitionReader(info, qp.storage, qp.primaryKeyField, HdxScanPartition(qp.db, qp.table, path, qp.cols, preds, hdxCols))
      while (pr.next()) {
        val row = pr.get()
        count += 1
        val rowTimestamp = DateTimeUtils.microsToInstant(row.getLong(timestampPos))

        if (rowTimestamp.compareTo(qp.minTimestamp) >= 0 && rowTimestamp.compareTo(qp.maxTimestamp) <= 0) {
          val otherValuesMatch = otherArgPoss.forall {
            case (pos, "null") =>
              // Special-case "foo=null"
              row.isNullAt(pos)
            case (pos, value) =>
              if (row.isNullAt(pos)) {
                false
              } else {
                row.getString(pos) == value
              }
          }

          if (otherValuesMatch) {
            written += 1
            writer.writeRow(rowToCsv(qp.cols, row))
          }
        }
      }
      pr.close()
    }

    logger.info(s"WORKER-$workerId: Scanned $count records; written $written (${count - written} filtered out)")

    writer.close()
    val dataLen = tmp.length()
    val execResp = ExecuteResponseMeta(true)
    writeChunk(System.out, JSON.objectMapper.writeValueAsBytes(execResp), Some((dataLen.toInt, new FileInputStream(tmp))))
  }

  private def rowToCsv(schema: StructType, row: InternalRow): List[String] = {
    for ((field, i) <- schema.fields.zipWithIndex.toList) yield {
      get(row, i, field.dataType)
    }
  }

  private def get(row: InternalRow, i: Int, typ: DataType): String = {
    if (row.isNullAt(i)) {
      null
    } else typ match {
      case DataTypes.BooleanType => row.getBoolean(i).toString
      case DataTypes.StringType => row.getString(i)
      case DataTypes.LongType => row.getLong(i).toString
      case DataTypes.IntegerType => row.getInt(i).toString
      case DataTypes.ShortType => row.getShort(i).toString
      case DataTypes.ByteType => row.getByte(i).toString
      case DataTypes.DoubleType => row.getDouble(i).toString
      case DataTypes.FloatType => row.getFloat(i).toString
      case DataTypes.TimestampType | DataTypes.TimestampNTZType =>
        val micros = row.getLong(i)
        val inst = DateTimeUtils.microsToInstant(micros)
        DateTimeFormatter.ISO_OFFSET_DATE_TIME.format(inst.atOffset(ZoneOffset.UTC))
      case dt: DecimalType =>
        row.getDecimal(i, dt.precision, dt.scale).toString()
      case other =>
        // TODO arrays, maps
        sys.error(s"Can't serialize $other values")
    }
  }

  private def spark2Hdx(name: String, dataType: DataType, pkField: String, pkType: HdxValueType): HdxColumnDatatype = {
    dataType match {
      case DataTypes.StringType =>
        HdxColumnDatatype(HdxValueType.String, index = true, primary = false)

      case DataTypes.FloatType | DataTypes.DoubleType =>
        HdxColumnDatatype(HdxValueType.Double, index = false, primary = false)

      case DataTypes.ByteType | DataTypes.ShortType | DataTypes.IntegerType =>
        // TODO maybe byte => Int8?
        HdxColumnDatatype(HdxValueType.Int32, index = true, primary = false)

      case DataTypes.LongType =>
        HdxColumnDatatype(HdxValueType.Int64, index = true, primary = false)

      case dt: DecimalType if dt.precision == 20 && dt.scale == 0 =>
        HdxColumnDatatype(HdxValueType.UInt64, index = true, primary = false)

      case DataTypes.TimestampType if name == pkField =>
        HdxColumnDatatype(pkType, index = true, primary = true)

      case DataTypes.TimestampType =>
        HdxColumnDatatype(pkType, index = true, primary = false)

      case DataTypes.BooleanType =>
        HdxColumnDatatype(HdxValueType.Boolean, index = true, primary = false)

      case ArrayType(elementType, _) =>
        val elt = spark2Hdx("n/a", elementType, "n/a", HdxValueType.DateTime64)
        HdxColumnDatatype(HdxValueType.Array, index = false, primary = false, elements = Some(List(elt)))

      case MapType(keyType, valueType, _) =>
        val kt = spark2Hdx("n/a", keyType, "n/a", HdxValueType.DateTime64)
        val vt = spark2Hdx("n/a", valueType, "n/a", HdxValueType.DateTime64)
        HdxColumnDatatype(HdxValueType.Map, index = false, primary = false, elements = Some(List(kt, vt)))

      case other =>
        sys.error(s"Don't know how to convert $name: $other")
    }
  }
}
