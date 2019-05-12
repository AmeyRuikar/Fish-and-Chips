
import org.apache.spark.sql.SparkSession

object StreamProcessing {

  def main(args: Array[String]) {

    val spark = SparkSession.builder.appName("Simple Application").getOrCreate()
    val sc = spark.sparkContext

    


    spark.stop()
  }
}