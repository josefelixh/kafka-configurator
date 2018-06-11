package com.sky.kafka.configurator

import java.io.{Reader => JReader}

import cats.implicits._
import io.circe
import io.circe.generic.AutoDerivation
import io.circe.yaml.parser._
import io.circe.{Decoder, DecodingFailure, Json}

import scala.collection.immutable.ListMap

object TopicConfigurationParser extends AutoDerivation {

  def apply(topicConfigReader: JReader): Either[circe.Error, List[Topic]] =
    for {
      ymlAsJson <- parse(topicConfigReader)
      topicConfigs <- ymlAsJson.as[List[Topic]]
    } yield topicConfigs

  case class TopicConfig(partitions: Int, replication: Int, config: Map[String, String], acls: Option[List[Acl]])

  implicit val topicsDecoder: Decoder[List[Topic]] = Decoder.instance { cursor =>
    for {
      configMap <- cursor.as[ListMap[String, TopicConfig]]
      topics = {
        configMap.map { case (name, conf) => Topic(name, conf.partitions, conf.replication, conf.config, conf.acls.getOrElse(List.empty[Acl])) }
      }
    } yield topics.toList
  }

  implicit val stringMapDecoder: Decoder[Map[String, String]] = Decoder.instance { cursor =>
    def stringify(json: Json): Json = json.asNumber
      .map(num => Json.fromString(num.truncateToInt.toString))
      .getOrElse(json)

    def failWithMsg(msg: String) = DecodingFailure(msg, List.empty)

    for {
      jsonObj <- cursor.value.asObject.toRight(failWithMsg(s"${cursor.value} is not an object"))
      valuesAsJsonStrings = jsonObj.withJsons(stringify).toMap
      stringMap <- valuesAsJsonStrings
        .mapValues(json => json.asString.toRight(failWithMsg(s"$json is not a string")))
        .sequenceU
    } yield stringMap
  }

  implicit val controlDecoder: Decoder[Control] = Decoder.decodeString.emap {
    case "allow" => Allow.asRight
    case "deny" => Deny.asRight
    case _ => "Control value not allowed, valid option are [allow, deny]".asLeft
  }
}
