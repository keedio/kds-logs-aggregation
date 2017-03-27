package org.keedio.kds.logs

import java.util
import java.util.Properties

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

    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    env.enableCheckpointing(5000)

    // Se obtienen los eventos de kafka
    val inputStream: DataStream[String] = env.addSource(new FlinkKafkaConsumer08[String](
      properties.getRequired("kafka.topic.list"),
      new SimpleStringSchema(),
      propertiesKafkaConsumer)
    )

    inputStream.rebalance.print

    // Se realiza una comprobacion de si Kafka y ElasticSeach estan arrancados?????

    //    val jsonStrema: DataStream[Json] = wordsStream.map(new createJsonFromStringComoMeviene())
    //    def creafJosnlf...() = {
    //      wordsStream.map(string => {
    //        val parMetadataYPaylod = string.split(":")
    //
    //        val arrayMeatadatsos = parMetadataYPaylod(0).split("] [")
    //        val payload = parMetadataYPaylod(1)
    //
    //        Seq(("fecha", arrayMeatadatsos(0)), ("serveridad", arrayMeatadatsos(1))..... , "body", payload)
    //
    //        new JsonKeedio(elemento  de arrbia)
    //
    //        PreJson.feccha, Presjons.bodh, laldjl
    //      })
    //    }

    //    val xxx: (String, String, String) = parseMap(inputStream.toString)
    //val palabras = wordsStream.map(t => (t._) //inputStream.flatMap(value => value.split(",")).map(t => (t(1),1))

    //val ll = wordsStream.map(new GridToCoordinates)

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
      override def createIndexRequest(element: String, ctx: RuntimeContext): IndexRequest = {

        val inputXXXX: (String, String, String) = parseMap(element)
        //        val inputXXXX: DataStream[(String, String, String)] = element.map(line => parseMap(line))

        val json = new util.HashMap[String, String]
        json.put("data_1", inputXXXX._1)
        json.put("data_2", inputXXXX._2)
        json.put("data_3", inputXXXX._3)

        println("element: " + element)
        println("SENDING_1: " + inputXXXX._1)
        println("SENDING_2: " + inputXXXX._2)
//        println("SENDING_3: " + inputXXXX.map(terna => terna._3).toString)

        Requests.indexRequest.index("my-index3").`type`("my-type").source(json)

      }
    }))

    env.execute("Flink Kafka Example")
  }

  def parseMap(line: String): (String, String, String) = {
    val record: Array[String] = line.split(",")
//    val paso = record(0)
    (record(0), record(1), record(2))

  }

}
