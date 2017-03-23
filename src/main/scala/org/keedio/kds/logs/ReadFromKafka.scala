package org.keedio.kds.logs

import java.util
import java.util.Properties

import org.apache.flink.api.common.functions.RuntimeContext
import org.apache.flink.api.java.utils.ParameterTool
import org.apache.flink.streaming.api.datastream.DataStreamSource
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment
import org.apache.flink.streaming.connectors.elasticsearch.{ElasticsearchSink, IndexRequestBuilder}
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer08
import org.apache.flink.streaming.util.serialization.SimpleStringSchema
import org.elasticsearch.action.index.IndexRequest
import org.elasticsearch.client.Requests
import org.elasticsearch.common.transport.{InetSocketTransportAddress, TransportAddress}

/**
  * Created by ivanrozas on 17/3/17.
  */
object ReadFromKafka {

  def main(args: Array[String]): Unit = {
    val propertiesFile = "./src/main/resources/logsAggregation.properties"
    val properties = ParameterTool.fromPropertiesFile(propertiesFile)

    val propertiesKafkaConsumer = new Properties()
    propertiesKafkaConsumer.setProperty("bootstrap.servers", properties.getRequired("kafka.broker.list"))
    propertiesKafkaConsumer.setProperty("zookeeper.connect", properties.getRequired("zookeeper.server.list"))
    propertiesKafkaConsumer.setProperty("group.id", properties.getRequired("zookeeper.group.id"))

    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    env.enableCheckpointing(5000)

    val inputStream: DataStreamSource[String] = env.addSource(
      new FlinkKafkaConsumer08[String](properties.getRequired("kafka.topic.list"), new SimpleStringSchema(),
        propertiesKafkaConsumer))

    inputStream.rebalance.print

    val config: util.HashMap[String, String] = new util.HashMap[String, String]
    config.put("bulk.flush.max.actions", properties.getRequired("elasticSearch.bulk.flush.max.actions"))
    config.put("cluster.name", properties.getRequired("elasticSearch.cluster.name"))

    val transportAddress: util.ArrayList[TransportAddress] = new util.ArrayList[TransportAddress]
    transportAddress.add(new InetSocketTransportAddress(properties.getRequired("elasticSearch.host.name"),
      properties.getRequired("elasticSearch.port").toInt))

    inputStream.addSink(new ElasticsearchSink(config, transportAddress, new IndexRequestBuilder[String] {
      override def createIndexRequest(element: String, ctx: RuntimeContext): IndexRequest = {

        val json = new util.HashMap[String, String]
        json.put("data", element)

        println("SENDING: " + element)

        Requests.indexRequest
          .index("my-index2")
          .`type`("my-type")
          .source(json)

      }
    }))

    env.execute("Flink Kafka Example")
  }

}
