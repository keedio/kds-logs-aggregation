package org.keedio.kds.logs

import java.io.StringWriter
import java.util
import java.util.Properties

import com.fasterxml.jackson.databind.ObjectMapper
import org.apache.flink.api.common.functions.RuntimeContext
import org.apache.flink.api.java.utils.ParameterTool
import org.apache.flink.api.scala._
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
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
    propertiesKafkaConsumer.setProperty("auto.offset.reset", properties.getRequired("kafka.auto.offset.reset"))

    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    //env.enableCheckpointing(5000)

    // Se obtienen los eventos de kafka
    val inputStream: DataStream[String] = env.addSource(new FlinkKafkaConsumer08[String](
      properties.getRequired("kafka.topic.list"),
      new SimpleStringSchema(),
      propertiesKafkaConsumer)
    )

    //    inputStream.rebalance.print

    // Se realiza una comprobacion de si Kafka y ElasticSeach estan arrancados?????

    // contar que el numero de campos es el correcto
    // ver si alguno mandatory (esto tiene sentido????) no ha llegado

    // controlar de alguna manera el tema de multilineas
    // si llega sin '\n' hay que volver a ponerla????

    // formar el json a insertar en elastecsearch

    // Con figuaracion del cluster de ElasticSearch
    val config: util.HashMap[String, String] = new util.HashMap[String, String]
    config.put("bulk.flush.max.actions", properties.getRequired("elasticSearch.bulk.flush.max.actions"))
    config.put("cluster.name", properties.getRequired("elasticSearch.cluster.name"))

    val transportAddress: util.ArrayList[TransportAddress] = new util.ArrayList[TransportAddress]
    // Si hay mas de un nodo habria que ver la forma de acerlo automatico.....
    transportAddress.add(new InetSocketTransportAddress(
      properties.getRequired("elasticSearch.host.name"),
      properties.getRequired("elasticSearch.port").toInt))

    inputStream.addSink(new ElasticsearchSink(config, transportAddress, new IndexRequestBuilder[String] {
      override def createIndexRequest(line: String, ctx: RuntimeContext): IndexRequest = {

        val finalJSON = createJSON(line)

        val serviceName = getMetadataLog(line)

        Requests.indexRequest.index(serviceName(3).toLowerCase()).`type`("my-type").source(finalJSON)

      }
    }))

    env.execute("Flink Kafka Example")
  }

  def getParMetadataPayLoad(line: String): Array[String] = {
    val parMetadataPayLoad: Array[String] = line.toString.split("]:")
    parMetadataPayLoad
  }

  def getMetadataLog(line: String): Array[String] = {

    val parMetadataPayLoad = getParMetadataPayLoad(line)
    val arrayMetadatos: Array[String] = parMetadataPayLoad(0).split("] \\[")

    arrayMetadatos
  }

  def getPayLoad(line: String): String = {

    val parMetadataPayLoad = getParMetadataPayLoad(line)
    val payLoad: String = parMetadataPayLoad(1)

    payLoad

  }

  def createJSON(line: String): String = {

    val arrayMetadatos: Array[String] = getMetadataLog(line)
    val payLoad: String = getPayLoad(line)

    val metadatosJson = new LogPayLoad(arrayMetadatos(4), arrayMetadatos(5), payLoad)
    val logJson = new LogMetadataService(arrayMetadatos(0), arrayMetadatos(0),
      arrayMetadatos(1), arrayMetadatos(2), metadatosJson)

    val mapper = new ObjectMapper()
    val outLogJson = new StringWriter
    mapper.writeValue(outLogJson, logJson)

    val json = outLogJson.toString()

    println("json: " + json)

    json

  }

}
