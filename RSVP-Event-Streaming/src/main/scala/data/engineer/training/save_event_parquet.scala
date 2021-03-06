package data.engineer.training


import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, from_json}
import org.apache.spark.sql.streaming.Trigger
import org.slf4j.LoggerFactory
import scala.collection.JavaConversions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.streaming._



object rsvp_streaming2 {

  private val log = LoggerFactory.getLogger(getClass)

  def main(args: Array[String]) {


    //val streaming_app_config: RsvpConfig = RsvpConfigUtil.loadConfig(args(0))
    log.info("====================================================")
    // log.info(streaming_app_config.toString)
    log.info("====================================================")


    val spark = SparkSession.builder()
      .appName("deproject.spark_app_name")
      .config("spark.dynamicAllocation.enabled", "false") // default is true
      .config("spark.streaming.backpressure.enabled", "true")
      .config("spark.streaming.kafka.maxRatePerPartition", 100)
      .config("spark.sql.shuffle.partitions", 2) // def 200
      .getOrCreate()
    spark.sparkContext.setLogLevel("INFO")


    val df1=spark.readStream.format("kafka")
      .option("kafka.bootstrap.servers","ip-172-31-94-165.ec2.internal:9092")
      .option("subscribe","rsvp_pan")
      .option("startingOffsets","earliest")
      .option("failOnDataLoss","false")
      .load()
    val df2=df1.select("value")
    val df3=df2.withColumn("value",col("value").cast(StringType))


    val test=spark.read
      .format("json")
      .option("inferSchema",true)
      .load("/user/pan/DE_project/test.json")
    test.printSchema()

    val df5=df3.select(from_json(col("value"), test.schema).as("data"))
    df5.printSchema()
    df5.isStreaming

    val df6=df5.select(col("data.*"))
    df6.printSchema()

    df6.writeStream
      .trigger(Trigger.ProcessingTime("60 seconds"))
      .format("parquet")
      .option("path","/user/pan/DE_project/parquet2")
      .option("checkpointLocation","/user/pan/DE_project/parquet2/checkpoint")
      .outputMode("append")
      .start
      .awaitTermination()


  }
}
