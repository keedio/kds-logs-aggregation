package org.keedio.kds.logs

import java.util.Properties

import org.apache.flink.api.java.utils.ParameterTool
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer08
import org.apache.flink.streaming.util.serialization.SimpleStringSchema

/**
  * Created by ivanrozas on 17/3/17.
  */
object ReadFromKafka {

  def main(args: Array[String]): Unit = {
    val propertiesFile = "./src/main/resources/logsAggregation.properties"
    val properties = ParameterTool.fromPropertiesFile(propertiesFile)


    val propertiesKafkaConsumer = new Properties()
    // comma separated list of Kafka brokers
    propertiesKafkaConsumer.setProperty("bootstrap.servers", properties.getRequired("kafka.broker.list"))
    // comma separated list of Zookeeper servers
    propertiesKafkaConsumer.setProperty("zookeeper.connect", properties.getRequired("zookeeper.server.list"))
    // the id of the consumer group
    propertiesKafkaConsumer.setProperty("group.id", "test")


    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val stream = env
      .addSource(new FlinkKafkaConsumer08[String](properties.getRequired("kafka.topic.list"), new SimpleStringSchema(),
        propertiesKafkaConsumer))
      .print()

    env.execute("Flink Kafka Example")
  }

}
