//package com.quantifind.kafka.core
//
//import com.quantifind.utils.ZkUtilsWrapper
//import org.I0Itec.zkclient.ZkClient
//import org.apache.zookeeper.data.Stat
//import org.mockito.Matchers._
//import org.mockito.Mockito._
//import org.mockito.{Matchers => MockitoMatchers, Mockito}
//import org.scalatest._
//
//class StormOffsetGetterSpec extends FlatSpec with ShouldMatchers {
//
//  trait Fixture {
//
//    val mockedZkClient = Mockito.mock(classOf[ZkClient])
//    val zkOffsetBase = "/stormconsumers"
//    val mockedZkUtil =  Mockito.mock(classOf[ZkUtilsWrapper])
//
//    val offsetGetter = new StormOffsetGetter(mockedZkClient, zkOffsetBase, mockedZkUtil)
//  }
//
//  "StormOffsetGetter" should "be able to extract topic from persisted spout state" in new Fixture {
//
//    val testGroup = "testgroup"
//    val testTopic = "testtopic"
//    val spoutState = s"""{
//                        "broker": {
//                            "host": "kafka.sample.net",
//                            "port": 9092
//                        },
//                        "offset": 4285,
//                        "partition": 1,
//                        "topic": "${testTopic}",
//                        "topology": {
//                            "id": "fce905ff-25e0 -409e-bc3a-d855f 787d13b",
//                            "name": "Test Topology"
//                        }
//                       }"""
//    val ret = (spoutState, Mockito.mock(classOf[Stat]))
//    when(mockedZkUtil.readData(MockitoMatchers.eq(mockedZkClient), anyString)).thenReturn(ret)
//
//    val topics = offsetGetter.getTopicList(testGroup)
//
//    topics.size shouldBe 1
//    topics(0) shouldBe testTopic
//  }
//}
