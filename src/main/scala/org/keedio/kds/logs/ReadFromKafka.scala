package org.keedio.kds.logs

import java.io.StringWriter
import java.util
import java.util.Properties

import com.fasterxml.jackson.databind.{JsonNode, ObjectMapper}
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

import scala.collection.JavaConversions._

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
      properties.getRequired("kafka.topic.list").split(",").toList,
      new SimpleStringSchema(),
      propertiesKafkaConsumer)
    )

    // Se realiza una comprobacion de si Kafka y ElasticSeach estan arrancados?????
    // Se comprueba que el indice existe en ElasticSeach

    // controlar de alguna manera el tema de multilineas
    // si llega sin '\n' hay que volver a ponerla????

    // Configuaracion del cluster de ElasticSearch
    val config: util.HashMap[String, String] = new util.HashMap[String, String]
    config.put("bulk.flush.max.actions", properties.getRequired("elasticSearch.bulk.flush.max.actions"))
    config.put("cluster.name", properties.getRequired("elasticSearch.cluster.name"))

    // Configuracion el 'transportAddress' del Sink de ElasticSearch
    // Si hay mas de un nodo habria que ver la forma de acerlo automatico.....
    val transportAddress: util.ArrayList[TransportAddress] = new util.ArrayList[TransportAddress]
    transportAddress.add(new InetSocketTransportAddress(
      properties.getRequired("elasticSearch.host.name"),
      properties.getRequired("elasticSearch.port").toInt))

    // Se manda a ElasticSearch el la informacion extraida del log en un JSON
    inputStream.addSink(new ElasticsearchSink(config, transportAddress, new IndexRequestBuilder[String] {
      override def createIndexRequest(lineEvent: String, ctx: RuntimeContext): IndexRequest = {

        // Se comprueba que la linea del evento llaga correctamente formada
        val lineLogOK = isJSON(lineEvent)

        val (finalJSON, nameServiceLog) = lineLogOK match {
          case true => {
            // Se obtiene el JSON como objeto
            val eventJSON = getJSONAsObject(lineEvent)

            //(createJSONromJSON(eventJSON), eventJSON.get("serviceName").toString)
            (createJSONFromJSON(eventJSON), "yarn")
          }
          case false => (createDummyJSON(lineEvent), "yarn")
        }

        println("******************************************** iniJSON: " + lineEvent)
        println("******************************************** finJSON: " + finalJSON)
        println("************************************* nameServiceLog: " + nameServiceLog.toLowerCase())

        Requests.indexRequest.index(properties.getRequired("elasticSearch.index.name"))
          .`type`(nameServiceLog.toLowerCase()).source(finalJSON)

      }
    }))

    env.execute("Flink kds-logs-aggregation")
  }

  def isJSON(line: String): Boolean = {
    try {
      getJSONAsObject(line)
      true

    } catch {
      case e: Exception => false
    }
  }

  def getJSONAsObject(line: String): JsonNode = {
    // Se transforma el String en un JSON
    val mapperRead = new ObjectMapper()
    val lineObj = mapperRead.readTree(line)

    lineObj
  }

  def createJSONFromJSON(lineObj: JsonNode): StringWriter = {

    val thread = lineObj.get("threadName").toString
    val fqcn = lineObj.get("locationInfo").get("className").toString
    val PayLoad = lineObj.get("message") + " " + lineObj.get("locationInfo").get("fullInfo") + " " +
      lineObj.get("throwableInfo").get("throwableStrRep")

    val datetime = lineObj.get("timeStamp").toString
    val timezone = "+0000"
    val hostname = "hostName"
    val level = lineObj.get("level").toString

    // Se crea la estructura del JSON
    val metaDatosJson = new LogPayLoad(thread, fqcn, PayLoad)
    val logJson = new LogMetadataService(datetime, timezone, hostname, level, metaDatosJson)

    // Se transforma el pojo en un JSON
    val mapperWrite = new ObjectMapper()
    val outLogJson = new StringWriter
    mapperWrite.writeValue(outLogJson, logJson)

    outLogJson
  }

  def createDummyJSON(line: String): StringWriter = {
    // Se crea la estructura del JSON
    val metadatosJson = new LogPayLoad("logMalformed", "logMalformed", line)
    val logJson = new LogMetadataService("logMalformed", "logMalformed", "logMalformed", "logMalformed", metadatosJson)

    // Se transforma el pojo en un JSON
    val mapper = new ObjectMapper()
    val outLogJson = new StringWriter
    mapper.writeValue(outLogJson, logJson)

    outLogJson
  }

}