package org.keedio.kds.logs

import com.fasterxml.jackson.core.JsonParseException
import com.fasterxml.jackson.databind.{JsonNode, ObjectMapper}
import org.junit.{Assert, Test}
import org.keedio.kds.logs.ReadFromKafka._
import org.slf4j.{Logger, LoggerFactory}

/**
  * Created by ivanrozas on 17/3/17.
  */
class ReadFromKafkaTest {

  val log: Logger = LoggerFactory.getLogger(this.getClass)

  /**
    * test: the string is a JSON
    */
  @Test
  def isJSONTest() = {
    Assert.assertTrue(isJSON("{\"datetime\":\"1491912976862\",\"timezone\":\"+0000\"}"))
  }

  /**
    * test: the string is not a JSON
    */
  @Test
  def isNotJSONTest() = {
    Assert.assertFalse(isJSON("the string is not a JSON"))
  }

  /**
    * test: the input is null
    *
    */
  @Test
  def isNullJSONTest() = {
    Assert.assertFalse(isJSON(null))
  }

  /**
    * test: the string is a JSON
    */
  @Test
  def getJSONAsObjectTest() = {
    val mapperRead = new ObjectMapper()
    val aaa = mapperRead.readTree("{\"datetime\":\"1491912976862\",\"timezone\":\"+0000\"}")

    val sss: JsonNode = getJSONAsObject("{\"datetime\":\"1491912976862\",\"timezone\":\"+0000\"}")
    Assert.assertEquals(aaa, sss)
  }

  /**
    * test: the string is not a JSON
    */
  @Test(expected = classOf[JsonParseException])
  def getNotJSONAsObjectTest() = {
    val test = getJSONAsObject("the string is not a JSON")
    //    Assert.fail()
  }

  /**
    * test: the null is not a JSON
    *
    */
  @Test(expected = classOf[JsonParseException])
  def getNullJSONAsObjectTest() = {
    val test = getJSONAsObject(null)
    //    Assert.fail()
  }
}