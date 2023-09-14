package io.hydrolix.splunk

import io.hydrolix.spark.connector.{HdxScanPartition, uuid0}
import io.hydrolix.spark.model.{HdxColumnDatatype, HdxColumnInfo, HdxConnectionInfo, HdxValueType, JSON}

import org.apache.curator.framework.CuratorFrameworkFactory
import org.apache.curator.framework.recipes.leader.LeaderLatch
import org.apache.curator.retry.ExponentialBackoffRetry
import org.apache.spark.sql.HdxPushdown.{GetField, Literal}
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.util.DateTimeUtils
import org.apache.spark.sql.connector.expressions.filter.{And, Predicate}
import org.apache.spark.sql.types._
import org.slf4j.{Logger, LoggerFactory}

import java.net.URI
import java.time.Instant
import java.util.UUID
import java.util.concurrent.LinkedBlockingQueue
import scala.collection.mutable
import scala.jdk.CollectionConverters._

object HdxQueryCommand {
  private val logger = LoggerFactory.getLogger(getClass)

  // TODO make this stuff configurable
  private val configName = "default"
  private val ZookeeperInitialDelay = 500
  private val MaxZookeeperAttempts = 10
  private val LeadershipInitialWait = 2000
  private val LeadershipRetryWait = 1000
  private val MaxLeadershipAttempts = 5
  private val MaxPlanAttempts = 30
  private val PlanAttemptWait = 1000
  private val MaxScanAttempts = 10
  private val ScanAttemptWait = 1000
  private val NumReaderThreads = 4

  private val remoteSidR = """^remote_.*?_([\d.]+)$""".r
  private val cleanSidR = """^([\d.]+)$""".r

  val partitionsDoneSignal = HdxScanPartition("", "", "", uuid0, StructType(Nil), Nil, Map())
  val outputDoneSignal = InternalRow.empty

  def main(args: Array[String]): Unit = {
    val getInfoMeta = readInitialChunk(System.in)
    logger.info(s"INIT: getinfo metadata: $getInfoMeta")

    val kvStoreAccess = if (args.length >= 3) {
      // TODO see if we can use splunk's secret management
      logger.info("INIT: Accessing KVStore with basic auth")
      KVStorePassword(new URI(args(0)), args(1), args(2))
    } else {
      logger.info("INIT: Accessing KVStore with session key")
      KVStoreSessionKey(new URI(getInfoMeta.searchInfo.splunkdUri), getInfoMeta.searchInfo.sessionKey)
    }

    val getInfoResponse = GetInfoResponseMeta(
      CommandType.events,
      generating = true,
      Nil,
      Some(600),
      "",
      finished = false,
      "",
      InspectorMessages(Nil)
    )

    writeChunk(System.out, JSON.objectMapper.writeValueAsBytes(getInfoResponse), None)

    readChunk(System.in) match {
      case (None, _) => sys.exit(0)
      case (Some(execMeta), execData) => // TODO pay attention to what's in the `execute` request someday maybe
        val nodeId = UUID.randomUUID()

        logger.info(s"INIT-$nodeId: execute metadata: $execMeta")
        logger.info(s"INIT-$nodeId: execute data: $execData")

        val hdxConfig = Config.load(kvStoreAccess, configName)

        logger.info(s"INIT-$nodeId: config loaded: ${hdxConfig.copy(password = "[REDACTED]")}")

        val now = System.currentTimeMillis()

        val sid = getInfoMeta.searchInfo.sid match {
          case remoteSidR(s) => s
          case cleanSidR(s) => s
          case other => sys.error(s"Couldn't parse search ID from $other")
        }

        val ll = doLeaderElection(nodeId, sid, hdxConfig.zookeeperServers)

        if (ll.hasLeadership) {
          logger.info(s"PLANNER-$nodeId: I'm the planner")

          val workerIds = ll.getParticipants.asScala.toList.map(p => UUID.fromString(p.getId))

          val minTimestamp = DateTimeUtils.microsToInstant((getInfoMeta.searchInfo.earliestTime * 1000000).toLong)
          val maxTimestamp = DateTimeUtils.microsToInstant((getInfoMeta.searchInfo.latestTime * 1000000).toLong)

          val plan = doPlan(minTimestamp, maxTimestamp, getInfoMeta.searchInfo.args, kvStoreAccess, nodeId, sid, hdxConfig.connectionInfo, now, workerIds)

          // Transition to worker mode
          logger.info(s"PLANNER-$nodeId: I'm a worker now")

          val scanJob = retry(
            logger, s"WORKER-$nodeId: Waiting for scan job",
            MaxScanAttempts, ScanAttemptWait,
            Config.readScanJob(kvStoreAccess, sid, nodeId)
          ).getOrElse(sys.error("Couldn't get scan job"))

          // TODO claim the scanJob

          // TODO make sure every ScanJob is claimed
          // TODO make sure every ScanJob is claimed
          // TODO make sure every ScanJob is claimed
          // TODO make sure every ScanJob is claimed

          doScan(nodeId, hdxConfig.connectionInfo, plan, scanJob.partitionPaths, scanJob.storageIds)
        } else {
          logger.info(s"WORKER-$nodeId: I'm a worker")

          val plan = retry(
            logger, s"WORKER-$nodeId: Waiting for query plan",
            MaxPlanAttempts, PlanAttemptWait,
            Config.readPlan(kvStoreAccess, sid)
          ).getOrElse(sys.error("Couldn't get query plan"))

          if (!plan.workerIds.contains(nodeId)) {
            logger.info(s"WORKER-$nodeId: Planner didn't have any work for me")
            writeChunk(System.out, JSON.objectMapper.writeValueAsBytes(ExecuteResponseMeta(finished = true)), None)
          } else {
            val scanJob = retry(
              logger, s"WORKER-$nodeId: Waiting for scan job",
              MaxScanAttempts, ScanAttemptWait,
              Config.readScanJob(kvStoreAccess, sid, nodeId)
            ).getOrElse(sys.error("Couldn't get scan job"))

            // TODO claim the scanJob

            doScan(nodeId, hdxConfig.connectionInfo, plan, scanJob.partitionPaths, scanJob.storageIds)
          }

          ll.close()
        }
    }
  }

  /**
   * Try to do the leader election with ZK
   *
   * @param nodeId           the ID of the current node
   * @param sid              the Splunk search ID
   * @param zookeeperServers the host:port strings for ZK servers
   * @return a LeaderLatch from which the current leader and any other participants can be queried
   */
  private def doLeaderElection(nodeId: UUID, sid: String, zookeeperServers: List[String]): LeaderLatch = {
    val curatorClient = CuratorFrameworkFactory.newClient(
      zookeeperServers.mkString(","),
      new ExponentialBackoffRetry(ZookeeperInitialDelay, MaxZookeeperAttempts)
    )

    curatorClient.start()

    val ll = new LeaderLatch(curatorClient, s"/search/$sid/planner", nodeId.toString)
    logger.info(s"INIT-$nodeId: Starting LeaderLatch")
    ll.start()
    logger.info(s"INIT-$nodeId: LeaderLatch started")

    var leadershipAttempts = 0
    var leaderId: UUID = null
    // Sleep first so workers have a chance to sign up
    Thread.sleep(LeadershipInitialWait)
    while (leaderId == null && leadershipAttempts < MaxLeadershipAttempts) {
      leadershipAttempts += 1
      val l = ll.getLeader
      if (l.getId.nonEmpty) {
        leaderId = UUID.fromString(l.getId)
      } else {
        Thread.sleep(LeadershipRetryWait)
      }
    }

    if (leaderId == null) sys.error(s"A Planner wasn't elected after $leadershipAttempts attempts")

    ll
  }

  private def doPlan(minTimestamp: Instant,
                     maxTimestamp: Instant,
                             args: List[String],
                    kvStoreAccess: KVStoreAccess,
                           nodeId: UUID,
                              sid: String,
                             info: HdxConnectionInfo,
                              now: Long,
                        workerIds: List[UUID]) =
  {
    val (plan, partitionPaths) = {
      val (dbName, tableName) = getDbTableArg(args)

      val cat = tableCatalog(info)

      val table = hdxTable(cat, dbName, tableName)

      val cols = getRequestedCols(args, table)
      logger.info(s"PLANNER-$nodeId: Requested columns: $cols")

      val otherTerms = getOtherTerms(args).toMap

      val predicates = List(new And(
        new Predicate(">=", Array(GetField(table.primaryKeyField), Literal(minTimestamp))),
        new Predicate("<=", Array(GetField(table.primaryKeyField), Literal(maxTimestamp))),
      )) ++ otherTerms.map {
        case (k, v) => new Predicate("=", Array(GetField(k), Literal(v)))
      }

      val predicatesBlob = compress(serialize(predicates))

      val hdxPartitions = planPartitions(table, cols, predicates, info)

      (
        QueryPlan(
          sid,
          None,
          System.currentTimeMillis(),
          nodeId,
          workerIds,
          dbName,
          tableName,
          table.primaryKeyField,
          table.hdxCols(table.primaryKeyField).hdxType.`type`,
          cols,
          table.storages,
          minTimestamp,
          maxTimestamp,
          otherTerms,
          predicatesBlob
        ),
        hdxPartitions.map(part => part.path -> part.storageId)
      )
    }

    Config.writePlan(kvStoreAccess, plan)

    val numWorkers = workerIds.size
    logger.info(s"PLANNER-$nodeId: Assigning ${partitionPaths.size} partitions to $numWorkers workers")
    val partitionsPerWorker = mutable.Map[UUID, Vector[(String, UUID)]]().withDefaultValue(Vector())

    // Allocate every partition that needs to be scanned to one of the workers
    for (((path, storageId), i) <- partitionPaths.zipWithIndex) {
      val worker = workerIds(i % numWorkers)
      partitionsPerWorker.update(worker, partitionsPerWorker(worker) :+ (path -> storageId))
    }

    // Write out the per-worker partition lists under the worker IDs
    for ((workerId, partitions) <- partitionsPerWorker) {
      logger.info(s"PLANNER-$nodeId: Worker $workerId should scan ${partitions.size} partitions")
      val (paths, storageIds) = partitions.toList.unzip
      Config.writeScanJob(
        kvStoreAccess,
        ScanJob(s"${sid}_$workerId", None, now, sid, workerId, claimed = false, paths, storageIds)
      )
    }

    plan
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

  private def doScan(workerId: UUID, info: HdxConnectionInfo, qp: QueryPlan, partitionPaths: List[String], storageIds: List[UUID]): Unit = {
    val jobQ = new LinkedBlockingQueue[HdxScanPartition](10)
    val rowQ = new LinkedBlockingQueue[InternalRow](1000)

    val preds = deserialize[List[Predicate]](decompress(qp.predicatesBlob))

    val hdxCols = qp.cols.fields.map { sf =>
      val hdxType = spark2Hdx(sf.name, sf.dataType, qp.primaryKeyField, qp.primaryKeyType)
      (
        sf.name,
        HdxColumnInfo(
          sf.name,
          hdxType,
          nullable = true,
          sf.dataType,
          2 // TODO we're assuming columns are always indexed
        )
      )
    }.toMap

    // Don't spawn more threads than partitions, but also don't spawn more than NumReaderThreads
    val numReaderThreads = partitionPaths.size.min(NumReaderThreads)

    val readerThreads = for (threadNo <- 1 to numReaderThreads) yield {
      new PartitionReaderThread(workerId, threadNo, info, qp, jobQ, rowQ)
    }
    readerThreads.foreach(_.start())

    val outputThread = new OutputWriterThread(workerId, qp, rowQ)
    outputThread.start()

    for ((path, storageId) <- partitionPaths.zip(storageIds)) {
      val scan = HdxScanPartition(qp.db, qp.table, path, storageId, qp.cols, preds, hdxCols)
      jobQ.put(scan)
    }

    // Tell every reader thread there are no more partitions, and wait for them all to exit
    readerThreads.foreach(_ => jobQ.put(partitionsDoneSignal))
    readerThreads.foreach(_.join())

    // Tell the output thread there are no more rows, and wait for it to exit
    rowQ.put(outputDoneSignal)
    outputThread.join()
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
