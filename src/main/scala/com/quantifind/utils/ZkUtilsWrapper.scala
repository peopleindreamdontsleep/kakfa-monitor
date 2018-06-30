package com.quantifind.utils

import kafka.api.LeaderAndIsr
import kafka.cluster.{Broker, Cluster}
import kafka.common.TopicAndPartition
import kafka.consumer.ConsumerThreadId
import kafka.controller.{LeaderIsrAndControllerEpoch, ReassignedPartitionsContext}
import kafka.utils.ZkUtils
import org.I0Itec.zkclient.ZkClient
import org.apache.zookeeper.data.Stat

import scala.collection.mutable

/*
  This class is mainly to help us mock the ZkUtils class. It is really painful to get powermock to work with scalatest,
  so we created this class with a little help from IntelliJ to auto-generate the delegation code
 */
class ZkUtilsWrapper {

  val ConsumersPath = ZkUtils.ConsumersPath

  //val delegator =  ZkUtils

  def getAllPartitions(zkUtils: ZkUtils): collection.Set[TopicAndPartition] = zkUtils.getAllPartitions()

  def getAllTopics(zkUtils: ZkUtils): Seq[String] = zkUtils.getAllTopics()

  def getBrokerInfo(zkUtils: ZkUtils, brokerId: Int): Option[Broker] = zkUtils.getBrokerInfo(brokerId)

  def getConsumersPerTopic(zkUtils: ZkUtils, group: String, excludeInternalTopics: Boolean): mutable.Map[String, List[ConsumerThreadId]] = zkUtils.getConsumersPerTopic(group, excludeInternalTopics)

  def getConsumersInGroup(zkUtils: ZkUtils, group: String): Seq[String] = zkUtils.getConsumersInGroup(group)

  def deletePartition(zkUtils: ZkUtils, brokerId: Int, topic: String): Unit = zkUtils.deletePartition(brokerId, topic)

  def getPartitionsUndergoingPreferredReplicaElection(zkUtils: ZkUtils): collection.Set[TopicAndPartition] = zkUtils.getPartitionsUndergoingPreferredReplicaElection()

  def updatePartitionReassignmentData(zkUtils: ZkUtils, partitionsToBeReassigned: collection.Map[TopicAndPartition, Seq[Int]]): Unit = zkUtils.updatePartitionReassignmentData(partitionsToBeReassigned)

  //def getPartitionReassignmentZkData(partitionsToBeReassigned: collection.Map[TopicAndPartition, Seq[Int]]): String = delegator.getPartitionReassignmentZkData(partitionsToBeReassigned)


  def parseTopicsData(jsonData: String): Seq[String] = ZkUtils.parseTopicsData(jsonData)

  def parsePartitionReassignmentData(jsonData: String): collection.Map[TopicAndPartition, Seq[Int]] = ZkUtils.parsePartitionReassignmentData(jsonData)

  def parsePartitionReassignmentDataWithoutDedup(jsonData: String): Seq[(TopicAndPartition, Seq[Int])] = ZkUtils.parsePartitionReassignmentDataWithoutDedup(jsonData)

  def getPartitionsBeingReassigned(zkUtils: ZkUtils): collection.Map[TopicAndPartition, ReassignedPartitionsContext] = zkUtils.getPartitionsBeingReassigned()

  def getPartitionsForTopics(zkUtils: ZkUtils, topics: Seq[String]): mutable.Map[String, Seq[Int]] = zkUtils.getPartitionsForTopics(topics)

  def getPartitionAssignmentForTopics(zkUtils: ZkUtils, topics: Seq[String]): mutable.Map[String, collection.Map[Int, Seq[Int]]] = zkUtils.getPartitionAssignmentForTopics(topics)

  def getReplicaAssignmentForTopics(zkUtils: ZkUtils, topics: Seq[String]): mutable.Map[TopicAndPartition, Seq[Int]] = zkUtils.getReplicaAssignmentForTopics(topics)

  def getPartitionLeaderAndIsrForTopics(zkUtils: ZkUtils, zKClient:ZkClient, topicAndPartitions: collection.Set[TopicAndPartition]): mutable.Map[TopicAndPartition, LeaderIsrAndControllerEpoch] = zkUtils.getPartitionLeaderAndIsrForTopics(zKClient,topicAndPartitions)

  def pathExists(client: ZkUtils, path: String): Boolean = client.pathExists( path)

  def getChildrenParentMayNotExist(client: ZkUtils, path: String): Seq[String] = client.getChildrenParentMayNotExist(path)

  def getChildren(client: ZkUtils, path: String): Seq[String] = client.getChildren(path)

  def readDataMaybeNull(client: ZkUtils, path: String): (Option[String], Stat) = client.readDataMaybeNull(path)

  def readData(client: ZkUtils, path: String): (String, Stat) = client.readData(path)

  def maybeDeletePath(zkUrl: String, dir: String): Unit = ZkUtils.maybeDeletePath(zkUrl, dir)

  def deletePathRecursive(client: ZkUtils, path: String): Unit = client.deletePathRecursive(path)

  def deletePath(client: ZkUtils, path: String): Boolean = client.deletePath(path)

  def updateEphemeralPath(client: ZkUtils, path: String, data: String): Unit = client.updateEphemeralPath(path, data)

  def conditionalUpdatePersistentPathIfExists(client: ZkUtils, path: String, data: String, expectVersion: Int): (Boolean, Int) = client.conditionalUpdatePersistentPathIfExists(path, data, expectVersion)

  def conditionalUpdatePersistentPath(client: ZkUtils, path: String, data: String, expectVersion: Int, optionalChecker: Option[(ZkUtils, String, String) => (Boolean, Int)]): (Boolean, Int) = client.conditionalUpdatePersistentPath(path, data, expectVersion, optionalChecker)

  def updatePersistentPath(client: ZkUtils, path: String, data: String): Unit = client.updatePersistentPath(path, data)

  def createSequentialPersistentPath(client: ZkUtils, path: String, data: String): String = client.createSequentialPersistentPath(path, data)

  def createPersistentPath(client: ZkUtils, path: String, data: String): Unit = client.createPersistentPath(path, data)

  //def createEphemeralPathExpectConflictHandleZKBug(zkUtils: ZkUtils, path: String, data: String, expectedCallerData: Any, checker: (String, Any) => Boolean, backoffTime: Int): Unit = delegator.createEphemeralPathExpectConflictHandleZKBug(ZkUtils, path, data, expectedCallerData, checker, backoffTime)

  def createEphemeralPathExpectConflict(client: ZkUtils, path: String, data: String): Unit = client.createEphemeralPathExpectConflict(path, data)

  def makeSurePersistentPathExists(client: ZkUtils, path: String): Unit = client.makeSurePersistentPathExists(path)

  def replicaAssignmentZkData(client: ZkUtils,map: collection.Map[String, Seq[Int]]): String = client.replicaAssignmentZkData(map)

  def leaderAndIsrZkData(client: ZkUtils,leaderAndIsr: LeaderAndIsr, controllerEpoch: Int): String = client.leaderAndIsrZkData(leaderAndIsr, controllerEpoch)

  def getConsumerPartitionOwnerPath(client: ZkUtils,group: String, topic: String, partition: Int): String = client.getConsumerPartitionOwnerPath(group, topic, partition)

  //def registerBrokerInZk(zkUtils: ZkUtils, id: Int, host: String, port: Int, timeout: Int, jmxPort: Int): Unit = zkUtils.registerBrokerInZk(id, host, port, timeout, jmxPort)

  def getReplicasForPartition(zkUtils: ZkUtils, topic: String, partition: Int): Seq[Int] = zkUtils.getReplicasForPartition(topic, partition)

  def getInSyncReplicasForPartition(zkUtils: ZkUtils, topic: String, partition: Int): Seq[Int] = zkUtils.getInSyncReplicasForPartition(topic, partition)

  def getEpochForPartition(zkUtils: ZkUtils, topic: String, partition: Int): Int = zkUtils.getEpochForPartition(topic, partition)

  def getLeaderForPartition(zkUtils: ZkUtils, topic: String, partition: Int): Option[Int] = zkUtils.getLeaderForPartition(topic, partition)

  def setupCommonPaths(zkUtils: ZkUtils): Unit = zkUtils.setupCommonPaths()

  def getLeaderAndIsrForPartition(zkUtils: ZkUtils, topic: String, partition: Int): Option[LeaderAndIsr] = zkUtils.getLeaderAndIsrForPartition(topic, partition)

  def getAllBrokersInCluster(zkUtils: ZkUtils): Seq[Broker] = zkUtils.getAllBrokersInCluster()

  def getSortedBrokerList(zkUtils: ZkUtils): Seq[Int] = zkUtils.getSortedBrokerList()

  def getTopicPartitionLeaderAndIsrPath(topic: String, partitionId: Int): String = ZkUtils.getTopicPartitionLeaderAndIsrPath(topic, partitionId)

  def getTopicPartitionPath(topic: String, partitionId: Int): String = ZkUtils.getTopicPartitionPath(topic, partitionId)

  def getController(zkUtils: ZkUtils): Int = zkUtils.getController()

  def getDeleteTopicPath(topic: String): String = ZkUtils.getDeleteTopicPath(topic)

  //def getTopicConfigPath(topic: String): String = ZkUtils.getTopicConfigPath(topic)

  def getTopicPartitionsPath(topic: String): String = ZkUtils.getTopicPartitionsPath(topic)

  def getTopicPath(topic: String): String = ZkUtils.getTopicPath(topic)

}
