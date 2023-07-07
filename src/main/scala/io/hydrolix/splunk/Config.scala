package io.hydrolix.splunk

import io.hydrolix.spark.model.{HdxConnectionInfo, JSON}

import java.net.http.HttpResponse.BodyHandlers
import java.net.http.{HttpClient, HttpRequest}
import java.net.{Socket, URI}
import java.security.cert.X509Certificate
import java.util.Base64
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

  def loadWithUserPass(url: URI, user: String, pass: String): HdxConnectionInfo = {
    val userPass = Base64.getEncoder.encodeToString(s"$user:$pass".getBytes("UTF-8"))
    load(url, "Basic", userPass)
  }

  def loadWithSessionKey(url: URI, sessionKey: String): HdxConnectionInfo = {
    load(url, "Splunk", sessionKey)
  }

  private def load(url: URI, realm: String, cred: String): HdxConnectionInfo = {
    // TODO maybe load other configs not named `default`?
    val getConfigs = HttpRequest
      .newBuilder(url.resolve("/servicesNS/nobody/hydrolix/storage/collections/data/hdx_config/default"))
      .setHeader("Authorization", s"$realm $cred")
      .build()

    val resp = client.send(getConfigs, BodyHandlers.ofString())
    val hc = JSON.objectMapper.readValue[HdxConfig](resp.body())

    HdxConnectionInfo(
      hc.jdbcUrl,
      hc.username,
      hc.password,
      hc.apiUrl,
      None,
      hc.cloudCred1,
      hc.cloudCred2
    )
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
