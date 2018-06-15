package common

import java.net.ServerSocket

import cakesolutions.kafka.testkit.KafkaServer
import kafka.utils.ZkUtils
import org.apache.kafka.clients.admin.AdminClient
import org.apache.kafka.clients.admin.AdminClientConfig._
import org.scalatest.BeforeAndAfterAll
import org.scalatest.concurrent.PatienceConfiguration

import scala.collection.JavaConverters._
import scala.concurrent.duration._

abstract class KafkaIntSpec extends BaseSpec with BeforeAndAfterAll with PatienceConfiguration {

  override implicit val patienceConfig = PatienceConfig(30 seconds, 250 millis)

  private val tls = this.getClass.getClassLoader.getResource("tls").getPath

  def randomAvailablePort(): Int = {
    val socket = new ServerSocket(0)
    val port = socket.getLocalPort
    socket.close()
    port
  }

  val sslPort = randomAvailablePort()
  val kafkaPort = randomAvailablePort()

  val kafkaServer = new KafkaServer(
    kafkaPort = kafkaPort,
    kafkaConfig = Map(
      "authorizer.class.name" -> "kafka.security.auth.SimpleAclAuthorizer",
      "ssl.truststore.location" -> s"$tls/kafka.server.truststore.jks",
      "ssl.truststore.password" -> s"testpassword",
      "ssl.keystore.location" -> s"$tls/kafka.server.keystore.jks",
      "ssl.keystore.password" -> s"testpassword",
      "ssl.key.password" -> s"testpassword",
      "listeners" -> s"PLAINTEXT://localhost:$kafkaPort,SSL://localhost:$sslPort",
      "ssl.client.auth" -> "requested",
      "allow.everyone.if.no.acl.found" -> "true"
    )
  )

  val zkSessionTimeout = 30 seconds
  val zkConnectionTimeout = 30 seconds


  lazy val zkUtils = ZkUtils(s"localhost:${kafkaServer.zookeeperPort}", zkSessionTimeout.toMillis.toInt,
    zkConnectionTimeout.toMillis.toInt, isZkSecurityEnabled = false)

  lazy val kafkaAdminClient = AdminClient.create(Map[String, AnyRef](
    BOOTSTRAP_SERVERS_CONFIG -> s"localhost:$kafkaPort",
    REQUEST_TIMEOUT_MS_CONFIG -> "5000"//,
//    SECURITY_PROTOCOL_CONFIG -> "PLAINTEXT",
//    "ssl.truststore.location" -> s"$tls/kafka.client.truststore.jks",
//    "ssl.truststore.password" -> s"testpassword",
//    "ssl.keystore.location" -> s"$tls/kafka.client.keystore.jks",
//    "ssl.keystore.password" -> s"testpassword",
//    "ssl.key.password" -> s"testpassword"
  ).asJava)

  override def beforeAll() = kafkaServer.startup()

  override def afterAll() = {
    kafkaAdminClient.close()
    kafkaServer.close()
  }

}