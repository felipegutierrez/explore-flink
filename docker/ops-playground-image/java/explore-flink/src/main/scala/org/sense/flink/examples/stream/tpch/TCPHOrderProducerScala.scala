package org.sense.flink.examples.stream.tpch

import java.io._
import java.nio.charset.StandardCharsets
import java.util.Properties

import org.apache.kafka.clients.producer._
import org.apache.kafka.common.serialization.StringSerializer
import org.sense.flink.examples.stream.tpch.pojo.Order
import org.sense.flink.util.DataRateListener
import org.sense.flink.util.MetricLabels.TPCH_DATA_ORDER
import org.slf4j.{Logger, LoggerFactory}

object TCPHOrderProducerScala {
  val topic: String = "tpc-h-order"
  val logger: Logger = LoggerFactory.getLogger(TCPHOrderProducerScala.getClass)
  val dataFilePath = TPCH_DATA_ORDER
  val dataRateListener: DataRateListener = new DataRateListener
  val bootstrapServers: String = "127.0.0.1:9092"
  var running: Boolean = false

  def main(args: Array[String]): Unit = {
    dataRateListener.start
    running = true
    // create properties
    val config: Properties = new Properties
    config.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers)
    config.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, classOf[StringSerializer].getName)
    config.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, classOf[StringSerializer].getName)
    // producer acks
    config.put(ProducerConfig.ACKS_CONFIG, "all");
    config.put(ProducerConfig.RETRIES_CONFIG, "3");
    config.put(ProducerConfig.LINGER_MS_CONFIG, "1");
    // leverage idempotent
    config.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, "true")
    // create producer
    val producer: KafkaProducer[String, String] = new KafkaProducer[String, String](config)

    run(producer)
  }

  def run(producer: KafkaProducer[String, String]): Unit = {
    var i = 0
    while (running) {
      logger.info("Producing batch: " + i)
      try {
        System.out.println("reading file: " + dataFilePath)
        var stream = new FileInputStream(dataFilePath)
        var reader = new BufferedReader(new InputStreamReader(stream, StandardCharsets.UTF_8))
        var rowNumber = 0
        var startTime = System.nanoTime
        var line = reader.readLine
        while (line != null) {
          rowNumber += 1
          val order = getOrderItem(line, rowNumber)
          // create producer record
          val record = new ProducerRecord[String, String](topic, String.valueOf(order.getOrderKey), order.getJson.toString)
          // send data asynchronous
          producer.send(record, new Callback() {
            override def onCompletion(recordMetadata: RecordMetadata, e: Exception): Unit = { // executes every time that a record is sent successfully
              if (e == null) { // the record was successfully sent
                logger.info("Received metadata: Topic: " + recordMetadata.topic + " Partition: " + recordMetadata.partition + " Offset: " + recordMetadata.offset + " Timestamp: " + recordMetadata.timestamp)
              }
              else logger.error("Error on sending message: " + e.getMessage)
            }
          })
          i += 1
          // sleep in nanoseconds to have a reproducible data rate for the data source
          this.dataRateListener.busySleep(startTime)
          // get start time and line for the next iteration
          startTime = System.nanoTime
          line = reader.readLine
        }
        reader.close()
        reader = null
        stream.close()
        stream = null
      } catch {
        case e: FileNotFoundException =>
          System.err.println("Please make sure they are available at [" + dataFilePath + "].")
          System.err.println(" Follow the instructions at [https://docs.deistercloud.com/content/Databases.30/TPCH%20Benchmark.90/Data%20generation%20tool.30.xml?embedded=true] in order to download and create them.")
          e.printStackTrace()
          this.running = false
        case e: IOException =>
          e.printStackTrace()
          this.running = false
      }
    }
    producer.close()
  }

  def getOrderItem(line: String, rowNumber: Int): Order = {
    val tokens = line.split("\\|")
    if (tokens.length != 9) throw new RuntimeException("Invalid record: " + line)

    try {
      val orderKey = tokens(0).toLong
      val customerKey = tokens(1).toLong
      val orderStatus = tokens(2).charAt(0)
      val totalPrice = tokens(3).toDouble.toLong
      val orderDate = tokens(4).replace("-", "").toInt
      val orderPriority = tokens(5)
      val clerk = tokens(6)
      val shipPriority = tokens(7).toInt
      val comment = tokens(8)
      val order = new Order(rowNumber, orderKey, customerKey, orderStatus, totalPrice, orderDate, orderPriority, clerk, shipPriority, comment)
      order
    } catch {
      case nfe: NumberFormatException =>
        throw new RuntimeException("Invalid record: " + line, nfe)
      case e: Exception =>
        throw new RuntimeException("Invalid record: " + line, e)
    }
  }
}
