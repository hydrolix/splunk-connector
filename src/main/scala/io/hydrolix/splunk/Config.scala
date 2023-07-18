package io.hydrolix.splunk

import io.hydrolix.spark.model.JSON

import java.net.http.HttpRequest.BodyPublishers
import java.net.http.HttpResponse.BodyHandlers
import java.net.http.{HttpClient, HttpRequest}
import java.net.{Socket, URI}
import java.security.cert.X509Certificate
import java.util.{Base64, UUID}
import javax.net.ssl.{SSLContext, SSLEngine, X509ExtendedTrustManager}

object Config {
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

  def loadWithUserPass(baseUrl: URI, user: String, pass: String): HdxConfig = {
    val userPass = Base64.getEncoder.encodeToString(s"$user:$pass".getBytes("UTF-8"))
    load(baseUrl, "Basic", userPass)
  }

  def loadWithSessionKey(baseUrl: URI, sessionKey: String): HdxConfig = {
    load(baseUrl, "Splunk", sessionKey)
  }

  private def load(baseUrl: URI, realm: String, cred: String): HdxConfig = {
    // TODO maybe load other configs not named `default`?
    val getConfigs = HttpRequest
      .newBuilder(baseUrl.resolve("/servicesNS/nobody/hydrolix/storage/collections/data/hdx_config/default"))
      .setHeader("Authorization", s"$realm $cred")
      .build()

    val resp = client.send(getConfigs, BodyHandlers.ofString())
    JSON.objectMapper.readValue[HdxConfig](resp.body())
  }

  // TODO we're assuming `sid` is globally unique across any number of search heads, that's probably not safe
  // TODO we're assuming `sid` is globally unique across any number of search heads, that's probably not safe
  // TODO we're assuming `sid` is globally unique across any number of search heads, that's probably not safe
  // TODO we're assuming `sid` is globally unique across any number of search heads, that's probably not safe
  def writePlan(baseUrl: URI, realm: String, cred: String, plan: QueryPlan): Unit = {
    val postPlan = HttpRequest.newBuilder(baseUrl.resolve("/servicesNS/nobody/hydrolix/storage/collections/data/hdx_plans/"))
      .POST(BodyPublishers.ofString(JSON.objectMapper.writeValueAsString(plan)))
      .setHeader("Authorization", s"$realm $cred")
      .build()

    client.send(postPlan, BodyHandlers.discarding())
  }

  def readPlan(baseUrl: URI, realm: String, cred: String, sid: String): Option[QueryPlan] = {
    val getPlan = HttpRequest.newBuilder(baseUrl.resolve(s"/servicesNS/nobody/hydrolix/storage/collections/data/hdx_plans/$sid"))
      .GET()
      .setHeader("Authorization", s"$realm $cred")
      .build()

    val resp = client.send(getPlan, BodyHandlers.ofString())
    resp.statusCode() match {
      case 200 =>
        Some(JSON.objectMapper.readValue[QueryPlan](resp.body()))
      case 404 =>
        None
      case other =>
        sys.error(s"GET query plan resulted in status $other")
    }
  }

  def readScanJob(baseUrl: URI, realm: String, cred: String, sid: String, workerId: UUID): Option[ScanJob] = {
    val getScanJob = HttpRequest.newBuilder(baseUrl.resolve(s"/servicesNS/nobody/hydrolix/storage/collections/data/hdx_scan_jobs/${sid}_$workerId"))
      .GET()
      .setHeader("Authorization", s"$realm $cred")
      .build()

    val resp = client.send(getScanJob, BodyHandlers.ofString())
    resp.statusCode() match {
      case 200 =>
        Some(JSON.objectMapper.readValue[ScanJob](resp.body()))
      case 404 =>
        None
      case other =>
        sys.error(s"GET scan job resulted in status $other")
    }
  }

  def writeScanJob(baseUrl: URI, realm: String, cred: String, job: ScanJob): Unit = {
    val postScan = HttpRequest.newBuilder(baseUrl.resolve(s"/servicesNS/nobody/hydrolix/storage/collections/data/hdx_scan_jobs/${job.sid}_${job.workerId}"))
      .POST(BodyPublishers.ofString(JSON.objectMapper.writeValueAsString(job)))
      .setHeader("Authorization", s"$realm $cred")
      .build()

    client.send(postScan, BodyHandlers.discarding())
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
