
import org.apache.spark._
import org.apache.spark.streaming._
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.streaming.kafka010._
import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent
import org.apache.spark.streaming.kafka010.ConsumerStrategies.Subscribe

object StreamProcessing {

  def main(args: Array[String]) {

    val conf = new SparkConf().setAppName("Fish-n-Chips app")
    val ssc = new StreamingContext(conf, Seconds(30))

    val kafkaParams = Map[String, Object](
                          "bootstrap.servers" -> "localhost:9092,",
                          "auto.offset.reset" -> "latest",
                          "key.deserializer" -> classOf[StringDeserializer],
                          "value.deserializer" -> classOf[StringDeserializer],
                          "group.id" -> "test-consumer-group",
                          "enable.auto.commit" -> (false: java.lang.Boolean))

    val topics = Array("fish-n-chips-orders")
    val stream = KafkaUtils.createDirectStream[String, String](
                                                ssc,
                                                PreferConsistent,
                                                Subscribe[String, String](topics, kafkaParams))
    val recordsRDD = stream.map(record => record.value)
    recordsRDD.print()

    val recByOrderId = recordsRDD.map(parseRecords)
    recByOrderId.print()
    
    ssc.start()
    ssc.awaitTermination()
  }

  def parseRecords(rec: String) {
    val parts = rec.split("""\|""")
    return (parts(1), (parts(0), parts(1), parts(2)))
  }
}