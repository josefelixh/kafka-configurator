package com.sky.kafka.configurator

import java.util.concurrent.ExecutionException

import cats.data.Reader
import com.sky.kafka.configurator.error.TopicNotFound
import org.apache.kafka.clients.admin.AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG
import org.apache.kafka.clients.admin._
import org.apache.kafka.common.acl._
import org.apache.kafka.common.config.ConfigResource
import org.apache.kafka.common.errors.{ SecurityDisabledException, UnknownTopicOrPartitionException }
import org.apache.kafka.common.resource.{ Resource, ResourceFilter, ResourceType }
import org.zalando.grafter.{ Stop, StopResult }

import scala.collection.JavaConverters._
import scala.language.postfixOps
import scala.util.{ Failure, Success, Try }

object KafkaTopicAdmin {
  def apply(adminClient: AdminClient): KafkaTopicAdmin = new KafkaTopicAdmin(adminClient)

  def reader: Reader[AppConfig, KafkaTopicAdmin] = Reader { config =>
    import com.sky.kafka.utils.MapToJavaPropertiesConversion.mapToProperties
    KafkaTopicAdmin(AdminClient.create(Map(BOOTSTRAP_SERVERS_CONFIG -> config.bootstrapServers)))
  }
}

class KafkaTopicAdmin(ac: AdminClient) extends TopicReader with TopicWriter with Stop {

  override def fetch(topicName: String) = {

    def topicDescription = Try {
      val allDescriptions = ac.describeTopics(Seq(topicName).asJava).all.get
      allDescriptions.get(topicName)
    } match {
      case Failure(e: ExecutionException) if e.getCause.isInstanceOf[UnknownTopicOrPartitionException] => Failure(TopicNotFound(topicName))
      case other => other
    }

    def topicConfig = Try {
      val allConfigs = ac.describeConfigs(Seq(configResourceForTopic(topicName)).asJava).all.get
      allConfigs.get(configResourceForTopic(topicName))
    }

    def aclsConfig = Try {
//      val rf = new ResourceFilter(ResourceType.TOPIC, topicName)
//      val acef = new AccessControlEntryFilter("test-client", "*", AclOperation.ANY, AclPermissionType.ANY)
      val description = ac.describeAcls(AclBindingFilter.ANY).values().get()
      println(description)
      description.asScala.map { inAcl =>
        Acl(
          inAcl.entry().principal(),
          inAcl.entry().host(),
          inAcl.entry().operation() match {
            case AclOperation.READ => true
            case _ => false
          },
          inAcl.entry().operation() match {
            case AclOperation.WRITE => true
            case _ => false
          },
          inAcl.entry().permissionType() match {
            case AclPermissionType.DENY => Deny
            case AclPermissionType.ALLOW => Allow
          }
        )

      } toList
    }

    for {
      desc <- topicDescription
      partitions = desc.partitions().size()
      replicationFactor = desc.partitions().asScala.head.replicas().size()
      config <- topicConfig
      acls <- aclsConfig
    } yield Topic(desc.name(), partitions, replicationFactor, config, acls)

  }

  override def create(topic: Topic) = Try {
    val newTopic = new NewTopic(topic.name, topic.partitions, topic.replicationFactor.toShort).configs(topic.config.asJava)
    ac.createTopics(Seq(newTopic).asJava).all().get
//    val acls = topic.acls.flatMap { acl =>
      val acls = Seq(new AclBinding(new Resource(ResourceType.TOPIC, topic.name), new AccessControlEntry(
        "test-client",
        "*",
        AclOperation.WRITE,
        AclPermissionType.ALLOW
      )))
//    }
    val newAcl = new AclBinding(new Resource(ResourceType.TOPIC, topic.name), new AccessControlEntry("my-user", "*", AclOperation.WRITE, AclPermissionType.ALLOW))
    ac.createAcls(List(newAcl).asJavaCollection)
//    ac.createAcls(acls.asJava).values().get()
  }

  override def updateConfig(topicName: String, config: Map[String, Object]) = Try {
    val c = config.map {
      case (key, value) => new ConfigEntry(key, value.toString)
    }.toList.asJava
    ac.alterConfigs(Map(configResourceForTopic(topicName) -> new Config(c)).asJava).all().get
  }

  override def updatePartitions(topicName: String, numPartitions: Int) = Try {
    ac.createPartitions(Map(topicName -> NewPartitions.increaseTo(numPartitions)).asJava).all().get()
  }

  override def stop = StopResult.eval("KafkaAdminClient")(ac.close())

  private def configResourceForTopic(topicName: String) = new ConfigResource(ConfigResource.Type.TOPIC, topicName)

  private implicit def kafkaConfigToMap(config: Config): Map[String, String] = config.entries().asScala.map { entry =>
    entry.name() -> entry.value()
  } toMap
}


