package com.quantifind.kafka.core

import java.nio.ByteBuffer

import com.quantifind.kafka.OffsetGetter
import com.quantifind.kafka.OffsetGetter.OffsetInfo
import com.quantifind.utils.ZkUtilsWrapper
import com.twitter.util.Time
import kafka.api.{OffsetRequest, PartitionOffsetRequestInfo}
import kafka.common.{OffsetAndMetadata, TopicAndPartition}
import kafka.consumer.{ConsumerConnector, KafkaStream, SimpleConsumer}
import kafka.message.MessageAndMetadata
import kafka.utils.{Logging, ZkUtils}
import org.I0Itec.zkclient.ZkClient

import scala.collection._
import scala.concurrent.Future
import scala.concurrent.ExecutionContext.Implicits.global
import scala.util.control.NonFatal

class KafkaOffsetGetter(zkUtils:ZkUtils,zkUtilsWrapper: ZkUtilsWrapper = new ZkUtilsWrapper) extends OffsetGetter {
  import KafkaOffsetGetter._

  //override val zkClient = theZkClient
  override val zkUtil=zkUtils
  override val zkWrapper=zkUtilsWrapper


  override def processPartition(group: String, topic: String, pid: Int): Option[OffsetInfo] = {
    try {
      zkWrapper.getLeaderForPartition(zkUtil,topic, pid) match {
        case Some(bid) =>
          val consumerOpt: Option[SimpleConsumer] = consumerMap.getOrElseUpdate(bid, getConsumer(bid))
          consumerOpt flatMap { consumer =>
              val topicAndPartition: TopicAndPartition = TopicAndPartition(topic, pid)
            //Map[GroupTopicPartition, OffsetAndMetadata]
              offsetMap.get(GroupTopicPartition(group, topicAndPartition)) map { offsetMetaData =>

                val request =
                  OffsetRequest(immutable.Map(topicAndPartition -> PartitionOffsetRequestInfo(OffsetRequest.LatestTime, 1)))
                val logSize = consumer.getOffsetsBefore(request).partitionErrorAndOffsets(topicAndPartition).offsets.head


                OffsetInfo(group = group,
                  topic = topic,
                  partition = pid,
                  offset = offsetMetaData.offset,
                  logSize = logSize,
                  owner = Some("NA"),
                  creation = Time.fromMilliseconds(offsetMetaData.commitTimestamp),
                  modified = Time.fromMilliseconds(offsetMetaData.commitTimestamp))
              }
          }
        case None =>
          error("No broker for partition %s - %s".format(topic, pid))
          None
      }
    } catch {
      case NonFatal(t) =>
        error(s"Could not parse partition info. group: [$group] topic: [$topic]", t)
        None
    }
  }

  override def getGroups: Seq[String] = {
    topicAndGroups.groupBy(_.group).keySet.toSeq
  }

  override def getTopicList(group: String): List[String] = {
    topicAndGroups.filter(_.group == group).groupBy(_.topic).keySet.toList
  }

  override def getTopicMap: Map[String, scala.Seq[String]] = {
    topicAndGroups.groupBy(_.topic).mapValues(_.map(_.group).toSeq)
  }

  override def getActiveTopicMap: Map[String, Seq[String]] = {
    getTopicMap
  }

  override def getTopics: Seq[String] = {
    topicAndGroups.groupBy(_.topic).map(each=>each._1).toSeq
  }
}

object KafkaOffsetGetter extends Logging {

  val ConsumerOffsetTopic = "__consumer_offsets"

  val offsetMap: mutable.Map[GroupTopicPartition, OffsetAndMetadata] = mutable.HashMap()
  val topicAndGroups: mutable.Set[TopicAndGroup] = mutable.HashSet()

  def startOffsetListener(consumerConnector: ConsumerConnector) = {

    Future {
      try {
        logger.info("Staring Kafka offset topic listener")
        val offsetMsgStream: KafkaStream[Array[Byte], Array[Byte]] = consumerConnector
          .createMessageStreams(Map(ConsumerOffsetTopic -> 1))
          .get(ConsumerOffsetTopic).map(_.head) match {
          case Some(s) => s
          case None => throw new IllegalStateException("Cannot create a consumer stream on offset topic ")
        }

        val it = offsetMsgStream.iterator()
        while (true) {
          try {
            val offsetMsg: MessageAndMetadata[Array[Byte], Array[Byte]] = it.next()
            val commitKey: GroupTopicPartition = readMessageKey(ByteBuffer.wrap(offsetMsg.key()))
            val commitValue: OffsetAndMetadata = readMessageValue(ByteBuffer.wrap(offsetMsg.message()))
            info("Processed commit message: " + commitKey + " => " + commitValue)
            offsetMap += (commitKey -> commitValue)

            topicAndGroups += TopicAndGroup(commitKey.topicPartition.topic, commitKey.group)

          } catch {
            case e: RuntimeException =>
              // sometimes offsetMsg.key() || offsetMsg.message() throws NPE
              warn("Failed to process one of the commit message due to exception. The 'bad' message will be skipped", e)
          }
        }
      } catch {
        case e: Throwable =>
          fatal("Offset topic listener aborted dur to unexpected exception", e)
          System.exit(1)
      }
    }
  }

  // massive code stealing from kafka.server.OffsetManager

  import java.nio.ByteBuffer

  import org.apache.kafka.common.protocol.types.Type.{INT32, INT64, STRING}
  import org.apache.kafka.common.protocol.types.{Field, Schema, Struct}


  private case class KeyAndValueSchemas(keySchema: Schema, valueSchema: Schema)

  private val OFFSET_COMMIT_KEY_SCHEMA_V0 = new Schema(new Field("group", STRING),
                                                       new Field("topic", STRING),
                                                       new Field("partition", INT32))
  private val KEY_GROUP_FIELD = OFFSET_COMMIT_KEY_SCHEMA_V0.get("group")
  private val KEY_TOPIC_FIELD = OFFSET_COMMIT_KEY_SCHEMA_V0.get("topic")
  private val KEY_PARTITION_FIELD = OFFSET_COMMIT_KEY_SCHEMA_V0.get("partition")

  private val OFFSET_COMMIT_VALUE_SCHEMA_V0 = new Schema(new Field("offset", INT64),
                                                         new Field("metadata", STRING, "Associated metadata.", ""),
                                                         new Field("timestamp", INT64))

  private val OFFSET_COMMIT_VALUE_SCHEMA_V1 = new Schema(new Field("offset", INT64),
                                                         new Field("metadata", STRING, "Associated metadata.", ""),
                                                         new Field("commit_timestamp", INT64),
                                                         new Field("expire_timestamp", INT64))

  private val VALUE_OFFSET_FIELD_V0 = OFFSET_COMMIT_VALUE_SCHEMA_V0.get("offset")
  private val VALUE_METADATA_FIELD_V0 = OFFSET_COMMIT_VALUE_SCHEMA_V0.get("metadata")
  private val VALUE_TIMESTAMP_FIELD_V0 = OFFSET_COMMIT_VALUE_SCHEMA_V0.get("timestamp")

  private val VALUE_OFFSET_FIELD_V1 = OFFSET_COMMIT_VALUE_SCHEMA_V1.get("offset")
  private val VALUE_METADATA_FIELD_V1 = OFFSET_COMMIT_VALUE_SCHEMA_V1.get("metadata")
  private val VALUE_COMMIT_TIMESTAMP_FIELD_V1 = OFFSET_COMMIT_VALUE_SCHEMA_V1.get("commit_timestamp")
  // private val VALUE_EXPIRE_TIMESTAMP_FIELD_V1 = OFFSET_COMMIT_VALUE_SCHEMA_V1.get("expire_timestamp")

  // map of versions to schemas
  private val OFFSET_SCHEMAS = Map(0 -> KeyAndValueSchemas(OFFSET_COMMIT_KEY_SCHEMA_V0, OFFSET_COMMIT_VALUE_SCHEMA_V0),
                                   1 -> KeyAndValueSchemas(OFFSET_COMMIT_KEY_SCHEMA_V0, OFFSET_COMMIT_VALUE_SCHEMA_V1))

  private def schemaFor(version: Int) = {
    val schemaOpt = OFFSET_SCHEMAS.get(version)
    schemaOpt match {
      case Some(schema) => schema
      case _ => throw new RuntimeException("Unknown offset schema version " + version)
    }
  }

  case class MessageValueStructAndVersion(value: Struct, version: Short)

  case class TopicAndGroup(topic: String, group: String)

  case class GroupTopicPartition(group: String, topicPartition: TopicAndPartition) {

    def this(group: String, topic: String, partition: Int) =
      this(group, new TopicAndPartition(topic, partition))

    override def toString =
      "[%s,%s,%d]".format(group, topicPartition.topic, topicPartition.partition)
  }

  /**
   * Decodes the offset messages' key
   *
   * @param buffer input byte-buffer
   * @return an GroupTopicPartition object
   */
  private def readMessageKey(buffer: ByteBuffer): GroupTopicPartition = {
    val version = buffer.getShort()
    val keySchema = schemaFor(version).keySchema
    val key = keySchema.read(buffer).asInstanceOf[Struct]

    val group = key.get(KEY_GROUP_FIELD).asInstanceOf[String]
    val topic = key.get(KEY_TOPIC_FIELD).asInstanceOf[String]
    val partition = key.get(KEY_PARTITION_FIELD).asInstanceOf[Int]

    GroupTopicPartition(group, TopicAndPartition(topic, partition))
  }

  /**
   * Decodes the offset messages' payload and retrieves offset and metadata from it
   *
   * @param buffer input byte-buffer
   * @return an offset-metadata object from the message
   */
  private def readMessageValue(buffer: ByteBuffer): OffsetAndMetadata = {
    val structAndVersion = readMessageValueStruct(buffer)

    if (structAndVersion.value == null) { // tombstone
      null
    } else {
      if (structAndVersion.version == 0) {
        val offset = structAndVersion.value.get(VALUE_OFFSET_FIELD_V0).asInstanceOf[Long]
        val metadata = structAndVersion.value.get(VALUE_METADATA_FIELD_V0).asInstanceOf[String]
        val timestamp = structAndVersion.value.get(VALUE_TIMESTAMP_FIELD_V0).asInstanceOf[Long]

        OffsetAndMetadata(offset, metadata, timestamp)
      } else if (structAndVersion.version == 1) {
        val offset = structAndVersion.value.get(VALUE_OFFSET_FIELD_V1).asInstanceOf[Long]
        val metadata = structAndVersion.value.get(VALUE_METADATA_FIELD_V1).asInstanceOf[String]
        val commitTimestamp = structAndVersion.value.get(VALUE_COMMIT_TIMESTAMP_FIELD_V1).asInstanceOf[Long]
        // not supported in 0.8.2
        // val expireTimestamp = structAndVersion.value.get(VALUE_EXPIRE_TIMESTAMP_FIELD_V1).asInstanceOf[Long]

        OffsetAndMetadata(offset, metadata, commitTimestamp)
      } else {
        throw new IllegalStateException("Unknown offset message version: " + structAndVersion.version)
      }
    }
  }

  private def readMessageValueStruct(buffer: ByteBuffer): MessageValueStructAndVersion = {
    if(buffer == null) { // tombstone
      MessageValueStructAndVersion(null, -1)
    } else {
      val version = buffer.getShort()
      val valueSchema = schemaFor(version).valueSchema
      val value = valueSchema.read(buffer).asInstanceOf[Struct]

      MessageValueStructAndVersion(value, version)
    }
  }
}