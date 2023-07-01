package io.hydrolix.splunk

import io.hydrolix.spark.model.{HdxConnectionInfo, JSON}

import java.net.{Socket, URI}
import java.net.http.HttpResponse.BodyHandlers
import java.net.http.{HttpClient, HttpRequest}
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

  def loadUserPass(user: String, pass: String): HdxConnectionInfo = {
    val userPass = Base64.getEncoder.encodeToString(s"$user:$pass".getBytes("UTF-8"))
    load("Basic", userPass)
  }

  def loadSessionKey(sessionKey: String): HdxConnectionInfo = {
    load("Splunk", sessionKey)
  }

  private def load(realm: String, cred: String): HdxConnectionInfo = {
    // TODO maybe load other configs not named `default`?
    // TODO will this always be localhost?
    val getConfigs = HttpRequest
      .newBuilder(new URI("https://localhost:8089/servicesNS/nobody/hydrolix/storage/collections/data/hdx_config/default"))
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
