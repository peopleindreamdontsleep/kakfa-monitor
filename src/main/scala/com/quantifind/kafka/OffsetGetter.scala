package com.quantifind.kafka

import java.util.Properties
import java.util.concurrent.atomic.AtomicBoolean

import com.quantifind.kafka.OffsetGetter.{BrokerInfo, KafkaInfo, OffsetInfo}
import com.quantifind.kafka.core.{KafkaOffsetGetter, ZKOffsetGetter}
import com.quantifind.kafka.offsetapp.OffsetGetterArgs
import com.quantifind.utils.ZkUtilsWrapper
import com.quantifind.utils.Utils
import com.twitter.util.Time
import kafka.common.BrokerNotAvailableException
import kafka.consumer.{Consumer, ConsumerConfig, ConsumerConnector, SimpleConsumer}
import kafka.utils.{Json, Logging, ZKStringSerializer, ZkUtils}
import org.I0Itec.zkclient.ZkClient


import scala.collection._
import scala.util.control.NonFatal

case class Node(name: String, children: Seq[Node] = Seq())

case class TopicDetails(consumers: Seq[ConsumerDetail])
case class TopicDetailsWrapper(consumers: TopicDetails)

case class TopicAndConsumersDetails(active: Seq[KafkaInfo], inactive: Seq[KafkaInfo])
case class TopicAndConsumersDetailsWrapper(consumers: TopicAndConsumersDetails)

case class ConsumerDetail(name: String)

trait OffsetGetter  extends Logging {

  val consumerMap: mutable.Map[Int, Option[SimpleConsumer]] = mutable.Map()
  def zkUtil: ZkUtils
  def zkWrapper:ZkUtilsWrapper

  //  kind of interface methods
  def getTopicList(group: String): List[String]
  def getGroups: Seq[String]
  def getTopicMap: Map[String, Seq[String]]
  def getActiveTopicMap: Map[String, Seq[String]]
  def processPartition(group: String, topic: String, pid: Int): Option[OffsetInfo]
  def getTopics: Seq[String]

  // get the Kafka simple consumer so that we can fetch broker offsets
  protected def getConsumer(bid: Int): Option[SimpleConsumer] = {
    try {
      if(bid < 0 ){
        return None
      }
      zkWrapper.readDataMaybeNull(zkUtil,ZkUtils.BrokerIdsPath + "/" + bid) match {
        case (Some(brokerInfoString), _) =>
          Json.parseFull(brokerInfoString) match {
            case Some(m) =>
              val brokerInfo = m.asInstanceOf[Map[String, Any]]
              val host = brokerInfo.get("host").get.asInstanceOf[String]
              val port = brokerInfo.get("port").get.asInstanceOf[Int]
              Some(new SimpleConsumer(host, port, 10000, 100000, "ConsumerOffsetChecker"))
            case None =>
              throw new BrokerNotAvailableException("Broker id %d does not exist".format(bid))
          }
        case (None, _) =>
          throw new BrokerNotAvailableException("Broker id %d does not exist".format(bid))
      }
    } catch {
      case t: Throwable =>
        error("Could not parse broker info", t)
        None
    }
  }

  protected def processTopic(group: String, topic: String): Seq[OffsetInfo] = {
    //[String,int]
    val pidMap = zkWrapper.getPartitionsForTopics(zkUtil,Seq(topic))
    for {
      partitions <- pidMap.get(topic).toSeq
      pid <- (partitions.sorted.filter(_>0))
      info <- processPartition(group, topic, pid)
    } yield info
  }

  protected def brokerInfo(): Iterable[BrokerInfo] = {
    for {
      (bid, consumerOpt) <- consumerMap
      consumer <- consumerOpt
    } yield BrokerInfo(id = bid, host = consumer.host, port = consumer.port)
  }

  protected def offsetInfo(group: String, topics: Seq[String] = Seq()): Seq[OffsetInfo] = {

    val topicList = if (topics.isEmpty) {
      getTopicList(group)
    } else {
      topics
    }

    topicList.sorted.flatMap(processTopic(group, _))
  }


  // get information about a consumer group and the topics it consumes
  def getInfo(group: String, topics: Seq[String] = Seq()): KafkaInfo = {
    val off = offsetInfo(group, topics)
    val brok = brokerInfo()
    KafkaInfo(
      name = group,
      brokers = brok.toSeq,
      offsets = off
    )
  }

  // get list of all topics
//  def getTopics: Seq[String] = {
//    try {
//      zkWrapper.getChildren(zkUtil,ZkUtils.BrokerTopicsPath).sortWith(_ < _)
//    } catch {
//      case NonFatal(t) =>
//        error(s"could not get topics because of ${t.getMessage}", t)
//        Seq()
//    }
//  }

  def getClusterViz: Node = {
    val clusterNodes = zkWrapper.getAllBrokersInCluster(zkUtil).map((broker) => {
      Node(Utils.getClusterList(broker.toString()), Seq())
    })
    Node("KafkaCluster", clusterNodes)
  }

  /**
   * Returns details for a given topic such as the consumers pulling off of it
   */
  def getTopicDetail(topic: String): TopicDetails = {
    val topicMap = getActiveTopicMap

    if (topicMap.contains(topic)) {
      TopicDetails(topicMap(topic).map(consumer => {
        ConsumerDetail(consumer.toString)
      }).toSeq)
    } else {
      TopicDetails(Seq(ConsumerDetail("Unable to find Active Consumers")))
    }
  }

  def mapConsumerDetails(consumers: Seq[String]): Seq[ConsumerDetail] =
    consumers.map(consumer => ConsumerDetail(consumer.toString))

  /**
   * Returns details for a given topic such as the active consumers pulling off of it
   * and for each of the active consumers it will return the consumer data
   */
  def getTopicAndConsumersDetail(topic: String): TopicAndConsumersDetailsWrapper = {
    val topicMap = getTopicMap
    val activeTopicMap = getActiveTopicMap

    val activeConsumers = if (activeTopicMap.contains(topic)) {
        mapConsumersToKafkaInfo(activeTopicMap(topic), topic)
    } else {
        Seq()
    }

    val inactiveConsumers = if (!activeTopicMap.contains(topic) && topicMap.contains(topic)) {
      mapConsumersToKafkaInfo(topicMap(topic), topic)
    } else if (activeTopicMap.contains(topic) && topicMap.contains(topic)) {
      mapConsumersToKafkaInfo(topicMap(topic).diff(activeTopicMap(topic)), topic)
    } else {
      Seq()
    }

    TopicAndConsumersDetailsWrapper(TopicAndConsumersDetails(activeConsumers, inactiveConsumers))
  }

  def mapConsumersToKafkaInfo(consumers: Seq[String], topic: String): Seq[KafkaInfo] =
    consumers.map(getInfo(_, Seq(topic)))


  def getActiveTopics: Node = {
    val topicMap = getActiveTopicMap

    Node("ActiveTopics", topicMap.map {
      case (s: String, ss: Seq[String]) => {
        Node(s, ss.map(consumer => Node(consumer)))

      }
    }.toSeq)
  }

  def close() {
    for (consumerOpt <- consumerMap.values) {
      consumerOpt match {
        case Some(consumer) => consumer.close()
        case None => // ignore
      }
    }
  }
}

object OffsetGetter {

  case class KafkaInfo(name: String, brokers: Seq[BrokerInfo], offsets: Seq[OffsetInfo])

  case class BrokerInfo(id: Int, host: String, port: Int)

  case class OffsetInfo(group: String,
                        topic: String,
                        partition: Int,
                        offset: Long,
                        logSize: Long,
                        owner: Option[String],
                        creation: Time,
                        modified: Time) {
    val lag = logSize - offset
  }

  val kafkaOffsetListenerStarted: AtomicBoolean = new AtomicBoolean(false)
  var zkClient: ZkClient = null
  var consumerConnector: ConsumerConnector = null
  var zkUtilClient:ZkUtils = null
  var zkUtilsWrapper = new ZkUtilsWrapper

  def createZKClient(args: OffsetGetterArgs): ZkClient = {
    new ZkClient(args.zk,
      args.zkSessionTimeout.toMillis.toInt,
      args.zkConnectionTimeout.toMillis.toInt
      )
  }

  def createZKUtils(args: OffsetGetterArgs):ZkUtils = {
   ZkUtils(args.zk,args.zkSessionTimeout.toMillis.toInt,args.zkConnectionTimeout.toMillis.toInt,false)
  }

  def createKafkaConsumerConnector(args: OffsetGetterArgs): ConsumerConnector = {
    val props: Properties = new Properties()
    // we want to be unique
    props.put("group.id", "KafkaOffsetMonitor-" + System.currentTimeMillis)
    props.put("zookeeper.connect", args.zk)
    // must enable it
    props.put("exclude.internal.topics", "false")
    // we don't want to commit any thing, will always start from latest
    props.put("auto.commit.enable", "false")
    props.put("auto.offset.reset", if (args.kafkaOffsetForceFromStart) "smallest" else "largest")

    Consumer.create(new ConsumerConfig(props))
  }

  def getInstance(args: OffsetGetterArgs): KafkaOffsetGetter = {

    if (kafkaOffsetListenerStarted.compareAndSet(false, true)) {
      zkUtilClient = createZKUtils(args)
      if (args.offsetStorage.toLowerCase == "kafka") {
        consumerConnector = createKafkaConsumerConnector(args)
        KafkaOffsetGetter.startOffsetListener(consumerConnector)
      }
    }

    args.offsetStorage.toLowerCase match {
      case "kafka" =>
        new KafkaOffsetGetter(zkUtilClient,zkUtilsWrapper)
      case _ =>
        null
    }
  }
}
