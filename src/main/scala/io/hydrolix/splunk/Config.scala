package io.hydrolix.splunk

import io.hydrolix.spark.model.JSON

import org.slf4j.LoggerFactory

import java.net.Socket
import java.net.http.HttpRequest.BodyPublishers
import java.net.http.HttpResponse.BodyHandlers
import java.net.http.{HttpClient, HttpRequest}
import java.security.cert.X509Certificate
import java.util.UUID
import javax.net.ssl.{SSLContext, SSLEngine, X509ExtendedTrustManager}

object Config {
  private val logger = LoggerFactory.getLogger(getClass)

  private val ctx = SSLContext.getInstance("TLS")
  // TODO make this optional!
  // TODO make this optional!
  // TODO make this optional!
  ctx.init(null, Array(InsecureTrustManager), null)
  // TODO make this optional!
  // TODO make this optional!
  // TODO make this optional!

  private val client = HttpClient.newBuilder()
    .sslContext(ctx)
    .build()

  def load(access: KVStoreAccess): HdxConfig = {
    // TODO maybe load other configs not named `default`?
    val getConfigs = HttpRequest
      .newBuilder(access.uri.resolve("/servicesNS/nobody/hydrolix/storage/collections/data/hdx_config/default"))
      .GET()
      .setHeader("Authorization", access.authHeaderValue)
      .build()

    val resp = client.send(getConfigs, BodyHandlers.ofString())
    JSON.objectMapper.readValue[HdxConfig](resp.body())
  }

  // TODO we're assuming `sid` is globally unique across any number of search heads, that's probably not safe
  // TODO we're assuming `sid` is globally unique across any number of search heads, that's probably not safe
  // TODO we're assuming `sid` is globally unique across any number of search heads, that's probably not safe
  // TODO we're assuming `sid` is globally unique across any number of search heads, that's probably not safe
  def writePlan(access: KVStoreAccess, plan: QueryPlan): Unit = {
    val postPlan = HttpRequest
      .newBuilder(access.uri.resolve("/servicesNS/nobody/hydrolix/storage/collections/data/hdx_plans/"))
      .POST(BodyPublishers.ofString(JSON.objectMapper.writeValueAsString(plan)))
      .setHeader("Authorization", access.authHeaderValue)
      .setHeader("Content-Type", "application/json")
      .build()

    val resp = client.send(postPlan, BodyHandlers.ofString())

    if (!Set(200, 201).contains(resp.statusCode())) sys.error(s"writePlan failed with status ${resp.statusCode()}; body was ${resp.body()}")
  }

  def readPlan(access: KVStoreAccess, sid: String): Option[QueryPlan] = {
    val getPlan = HttpRequest
      .newBuilder(access.uri.resolve(s"/servicesNS/nobody/hydrolix/storage/collections/data/hdx_plans/$sid"))
      .GET()
      .setHeader("Authorization", access.authHeaderValue)
      .build()

    val resp = client.send(getPlan, BodyHandlers.ofString())
    resp.statusCode() match {
      case 200 =>
        Some(JSON.objectMapper.readValue[QueryPlan](resp.body()))
      case 404 =>
        None
      case other =>
        sys.error(s"GET query plan resulted in status $other; body was ${resp.body()}")
    }
  }

  def writeScanJob(access: KVStoreAccess, job: ScanJob): Unit = {
    val postScan = HttpRequest
      .newBuilder(access.uri.resolve(s"/servicesNS/nobody/hydrolix/storage/collections/data/hdx_scan_jobs/${job.sid}_${job.workerId}"))
      .POST(BodyPublishers.ofString(JSON.objectMapper.writeValueAsString(job)))
      .setHeader("Authorization", access.authHeaderValue)
      .setHeader("Content-Type", "application/json")
      .build()

    val resp = client.send(postScan, BodyHandlers.ofString())

    if (!Set(200, 201).contains(resp.statusCode())) sys.error(s"writeScanJob failed with status ${resp.statusCode()}; body was ${resp.body()}")
  }

  def readScanJob(access: KVStoreAccess, sid: String, workerId: UUID): Option[ScanJob] = {
    val getScanJob = HttpRequest
      .newBuilder(access.uri.resolve(s"/servicesNS/nobody/hydrolix/storage/collections/data/hdx_scan_jobs/${sid}_$workerId"))
      .GET()
      .setHeader("Authorization", access.authHeaderValue)
      .build()

    val resp = client.send(getScanJob, BodyHandlers.ofString())
    resp.statusCode() match {
      case 200 =>
        Some(JSON.objectMapper.readValue[ScanJob](resp.body()))
      case 404 =>
        None
      case other =>
        sys.error(s"GET scan job resulted in status $other; body was ${resp.body()}")
    }
  }
}

object InsecureTrustManager extends X509ExtendedTrustManager {
  override def checkClientTrusted(chain: Array[X509Certificate], authType: String): Unit = ()
  override def checkServerTrusted(chain: Array[X509Certificate], authType: String): Unit = ()
  override def getAcceptedIssuers: Array[X509Certificate] = Array()
  override def checkClientTrusted(chain: Array[X509Certificate], authType: String, socket: Socket): Unit = ()
  override def checkServerTrusted(chain: Array[X509Certificate], authType: String, socket: Socket): Unit = ()
  override def checkClientTrusted(chain: Array[X509Certificate], authType: String, engine: SSLEngine): Unit = ()
  override def checkServerTrusted(chain: Array[X509Certificate], authType: String, engine: SSLEngine): Unit = ()
}
