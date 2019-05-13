
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

    val readyOrders = recByOrderId.mapWithState(
      StateSpec.function(mappingFunc _)
    ).filter(x => x._2 != true)
    
    readyOrders.print()

    ssc.start()
    ssc.awaitTermination()
  }

  def mappingFunc(key: String, newItem: Option[String], state: State[(Boolean, Boolean)]): (String, Boolean) = {

      val item = newItem.getOrElse(null)
            if(item == null) {
              return (key, false)
            }
            val prevState = state.get()

            if(prevState == (false, false)) {
              // state does not exist
              if (item.equals("fish")) {
                state.update((true, false))
              }
              else {
                state.update((false, true))
              }
              return (key, false)

            }
            else {
              // state exists
              if((item.equals("fish") && prevState == (false, true) ) || (item.equals("chips") && prevState == (true, false)) ) {
                state.remove()
                return (key, true)
              }
              else {
                  return (key, false)
              }

            }

  }

  def parseRecords(rec: String):(String, String) = {
    val parts = rec.split("""\|""")
    return (parts(1), parts(2))
  }
}