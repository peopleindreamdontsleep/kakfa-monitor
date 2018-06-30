package com.quantifind.kafka.core

import com.quantifind.kafka.Node
import com.quantifind.utils.ZkUtilsWrapper
import kafka.utils.ZkUtils

import scala.collection.Seq

object GetClusterList {

  def main(args: Array[String]): Unit = {

    val zkWrapper = new ZkUtilsWrapper

    val zkUtil = ZkUtils("dfttkafka01:2181,dfttkafka02:2181,dfttkafka03:2181",10000,1000,false)

    val clusterNodes = zkWrapper.getAllBrokersInCluster(zkUtil).map((broker) => {
      Node(broker.toString(), Seq())
    })
    Node("KafkaCluster", clusterNodes)
    println(clusterNodes)

  }

}
