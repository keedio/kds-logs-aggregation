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
      override def createIndexRequest(lineLog: String, ctx: RuntimeContext): IndexRequest = {

        // contar que el numero de campos es el correcto
        // ver si alguno mandatory (esto tiene sentido????) no ha llegado
        val lineLogOK = isLineOK(lineLog)

        val (finalJSON, nameServiceLog) = lineLogOK match {
          case true => (createJSON(lineLog), getMetadataLog(lineLog)(3))
          case false => (createDummyJSON(lineLog), "duumyService")
        }
        println("finalJSON: " + finalJSON.toString)

        Requests.indexRequest.index(properties.getRequired("elasticSearch.index.name"))
          .`type`(nameServiceLog.toLowerCase()).source(finalJSON)

      }
    }))

    env.execute("Flink kds-logs-aggregation")
  }

  def isLineOK(line: String): Boolean = {

    val parMetadataYPaylod: Array[String] = line.split("]:")

    parMetadataYPaylod.length match {
      case 2 => {
        val arrayMeatadatsos = parMetadataYPaylod(0).split("] \\[")
        arrayMeatadatsos.length match {
          case 6 => true
          case _ => false
        }
      }
      case _ => false
    }
  }

  def createDummyJSON(line: String): String = {
    // Se crea la estructura del JSON
    val metadatosJson = new LogPayLoad("logMalformed", "logMalformed", line)
    val logJson = new LogMetadataService("logMalformed", "logMalformed", "logMalformed", "logMalformed", metadatosJson)

    // Se transforma el pojo en un JSON
    val mapper = new ObjectMapper()
    val outLogJson = new StringWriter
    mapper.writeValue(outLogJson, logJson)

    outLogJson.toString()
  }

  def createJSON(line: String): String = {

    // Se obtienen los datos de la linea de log
    val arrayMetadatos: Array[String] = getMetadataLog(line)
    val payLoad: String = getPayLoad(line)

    val timeStampLog: String = getTimeStamp(arrayMetadatos(0).substring(1, arrayMetadatos(0).length))
    val timeZoneLog: String = getTimeZone(arrayMetadatos(0))

    // Se crea la estructura del JSON
    val metadatosJson = new LogPayLoad(arrayMetadatos(4), arrayMetadatos(5), payLoad)
    val logJson = new LogMetadataService(timeStampLog, timeZoneLog, arrayMetadatos(1), arrayMetadatos(2), metadatosJson)

    // Se transforma el pojo en un JSON
    val mapper = new ObjectMapper()
    val outLogJson = new StringWriter
    mapper.writeValue(outLogJson, logJson)

    outLogJson.toString()
  }

  def getTimeStamp(text: String): String = {
    val arraySplitText = getSplitText(text, " ")
    val timeStampLog = arraySplitText(0) + " " + arraySplitText(1)

    timeStampLog
  }

  def getTimeZone(text: String): String = {
    val arraySplitText = getSplitText(text, " ")
    val timeZoneLog = arraySplitText(2)

    timeZoneLog
  }

  def getSplitText(text: String, charSplit: String): Array[String] = {
    val arraySplitText: Array[String] = text.toString.split(charSplit)
    arraySplitText
  }

  def getMetadataLog(line: String): Array[String] = {
    val parMetadataPayLoad = getSplitText(line, "]:")
    val arrayMetadatos: Array[String] = parMetadataPayLoad(0).split("] \\[")

    arrayMetadatos
  }

  def getPayLoad(line: String): String = {
    val parMetadataPayLoad = getSplitText(line, "]:")
    val payLoad: String = parMetadataPayLoad(1)

    payLoad
  }

}