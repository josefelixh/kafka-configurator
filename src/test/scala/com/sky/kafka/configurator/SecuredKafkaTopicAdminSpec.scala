package com.sky.kafka.configurator

import java.util.UUID

import com.sky.kafka.matchers.TopicMatchers
import common.DockerCompose
import org.apache.kafka.clients.CommonClientConfigs.SECURITY_PROTOCOL_CONFIG
import org.apache.kafka.clients.admin.AdminClient
import org.apache.kafka.clients.admin.AdminClientConfig.{ BOOTSTRAP_SERVERS_CONFIG, CLIENT_ID_CONFIG }
import org.apache.kafka.common.acl.{ AccessControlEntry, AclBinding, AclOperation, AclPermissionType }
import org.apache.kafka.common.config.SslConfigs._
import org.apache.kafka.common.resource.{ Resource, ResourceType }
import org.scalatest.concurrent.Eventually
import org.scalatest.fixture

import scala.collection.JavaConverters._
import scala.concurrent.duration._
import scala.language.postfixOps
import scala.util.Success

class SecuredKafkaTopicAdminSpec extends fixture.FlatSpec with fixture.ConfigMapFixture with TopicMatchers with Eventually {

  implicit val pc = PatienceConfig(30 seconds, 1 seconds)

  "create" should "configure topic including acls for a secured kafka" taggedAs(DockerCompose) in { configMap =>
    val kafkaBootstrap = configMap.getOrElse("kafka-ssl:19093", "unknown:unknown")

    val sslCertsPassword = "confluent"
    val client = AdminClient.create(Map[String, AnyRef](
      BOOTSTRAP_SERVERS_CONFIG -> s"$kafkaBootstrap",
      CLIENT_ID_CONFIG -> "client",
      SECURITY_PROTOCOL_CONFIG -> "SSL",
      SSL_KEYSTORE_LOCATION_CONFIG -> "/Users/hernandj/disco/oss/kafka-configurator/docker/tls/kafka.client.keystore.jks",
      SSL_KEYSTORE_PASSWORD_CONFIG -> sslCertsPassword,
      SSL_TRUSTSTORE_LOCATION_CONFIG -> "/Users/hernandj/disco/oss/kafka-configurator/docker/tls/kafka.client.truststore.jks",
      SSL_TRUSTSTORE_PASSWORD_CONFIG -> sslCertsPassword,
      SSL_KEY_PASSWORD_CONFIG -> sslCertsPassword
    ).asJava)
    val adminClient = KafkaTopicAdmin(client)

    eventually {
      println(">>>>>>>>>")

      try {
        println(client.describeCluster().nodes().get())
        adminClient.create(someTopic) shouldBe Success(())
        println(client.listTopics().listings().get().asScala.foreach(println))
        println(client.createAcls(Seq(
          new AclBinding(new Resource(ResourceType.CLUSTER, client.describeCluster().clusterId().get()), new AccessControlEntry("producer.test.confluent.io", "*", AclOperation.ALL, AclPermissionType.ALLOW))
        ).asJavaCollection).all().get())
      } catch {
        case t: Throwable => println(t.getMessage)
      }

      println(client.createAcls(Seq(
        new AclBinding(new Resource(ResourceType.TOPIC, someTopic.name), new AccessControlEntry("producer.test.confluent.io", "*", AclOperation.ALL, AclPermissionType.ALLOW))
      ).asJavaCollection).all().get())

      println(">>>>>>>>>>>>>>")

      println(client.describeTopics(Seq(someTopic.name).asJavaCollection).all().get())
      println(">>>>>>>>>>>>>>>>>>>>")

      val createdTopic = adminClient.fetch(someTopic.name)
      println(">>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>")
      createdTopic.toEither.right.get should beEquivalentTo(someTopic)
    }
  }

  def someTopic = Topic(
    name = UUID.randomUUID().toString,
    partitions = 3,
    replicationFactor = 1,
    config = Map.empty[String, String],
    acls = Seq(
      Acl("aUser", "*", true, true, Allow)
    )
  )
}
