package com.sky.kafka

import java.io.File

import cats.data.WriterT
import cats.implicits._

import scala.util.{ Failure, Success, Try }

package object configurator {

  type Logger[T] = WriterT[Try, List[String], T]

  case class AppConfig(file: File = new File("."), bootstrapServers: String = "")

  case class Topic(name: String, partitions: Int, replicationFactor: Int, config: Map[String, String], acls: Seq[Acl] = Seq())
  case class Acl(user: String, group: String, consumer: Boolean, producer: Boolean, control: Control)
  sealed trait Control
  case object Allow extends Control
  case object Deny extends Control


  trait TopicReader {
    def fetch(topicName: String): Try[Topic]
  }

  trait TopicWriter {
    def create(topic: Topic): Try[Unit]

    def updateConfig(topicName: String, config: Map[String, Object]): Try[Unit]

    def updatePartitions(topicName: String, numPartitions: Int): Try[Unit]
  }

  implicit class TryLogger[T](val t: Try[T]) extends AnyVal {

    def withLog(log: String): Logger[T] = t match {
      case Success(_) =>
        liftTryAndWrite(log)
      case Failure(_) =>
        t.asWriter
    }

    private def liftTryAndWrite(msg: String): Logger[T] =
      WriterT.putT(t)(List(msg))

    def asWriter: Logger[T] =
      WriterT.valueT(t)
  }

}
