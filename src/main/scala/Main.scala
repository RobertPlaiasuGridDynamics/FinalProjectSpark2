import breeze.linalg.sum
import org.apache.log4j.{Level, Logger}
import org.apache.spark
import org.apache.spark.sql.{DataFrame, SparkSession, functions}
import org.apache.avro.Schema
import org.apache.parquet.Files
import org.apache.spark.sql.catalyst.dsl.expressions.StringToAttributeConversionHelper
import org.apache.spark.sql.functions.{col, count, last, lit, monotonically_increasing_id}
import org.apache.spark.sql.types.LongType

import java.nio.file.Paths
import java.io.File


object Main extends App{
  
  Logger.getLogger("org").setLevel(Level.ERROR)

  val spark = SparkSession
    .builder
    .appName("FinalProjectSpark")
    .master("local[*]")
    .getOrCreate()

  // retweet
  WriteJsonToAvro("data/Retweet.json","data/Retweet",spark)

  val retweet = ReadAvro.read("data/Retweet","data/Retweet.avsc",spark).as("retweet")

  // UserDir
  WriteJsonToAvro("data/UserDir.json","data/UserDir",spark)

  val userDir = ReadAvro.read("data/UserDir","data/UserDir.avsc",spark).as("users")
  userDir.createOrReplaceTempView("users")

  // message
  WriteJsonToAvro("data/Message.json","data/Message",spark)

  val message = ReadAvro.read("data/Message","data/Message.avsc",spark)



  // messageDir
  import spark.implicits._

  WriteJsonToAvro("data/MessageDir.json","data/MessageDir",spark)

  val messageDir = ReadAvro.read("data/MessageDir","data/MessageDir.avsc",spark).as("message")
  messageDir.createOrReplaceTempView("message")


  val retweetCountSubscriber = countMessageRetweets(retweet).as("retweetCount")

  def countMessageRetweets(retweet : DataFrame) = {
    retweet.groupBy("USER_ID", "MESSAGE_ID").agg(count("SUBSCRIBER_ID").alias("count"))
  }

  retweetCountSubscriber.createOrReplaceTempView("retweetCount")
  retweet.createOrReplaceTempView("retweet")

  val countAllRetweets = retweetCountSubscriber
    .join(retweet,
      col("SUBSCRIBER_ID") === col("retweetCount.USER_ID")
      && col("retweetCount.MESSAGE_ID") === col("retweet.MESSAGE_ID")
    )
    .union(
      retweetCountSubscriber
        .join(retweet,
          col("retweet.USER_ID") === col("retweetCount.USER_ID")
            && col("retweetCount.MESSAGE_ID") === col("retweet.MESSAGE_ID")
        ))
    .groupBy(col("retweetCount.USER_ID"),col("retweetCount.MESSAGE_ID"))
    .agg(functions.sum(col("count")).as("NUMBER_RETWEETS"))
    .select("NUMBER_RETWEETS","USER_ID","MESSAGE_ID")
    .sort(col("NUMBER_RETWEETS").desc)
    .as("simpleTopUser")


  countAllRetweets
    .join(messageDir,col("message.MESSAGE_ID") === col("simpleTopUser.MESSAGE_ID"))
    .join(userDir,col("users.USER_ID") === col("simpleTopUser.USER_ID"))
    .select("simpleTopUser.USER_ID","FIRST_NAME","LAST_NAME","simpleTopUser.MESSAGE_ID","TEXT","NUMBER_RETWEETS")
    .sort(col("NUMBER_RETWEETS").desc)
    .show(10)


}
