import breeze.linalg.sum
import org.apache.log4j.{Level, Logger}
import org.apache.spark
import org.apache.spark.sql.{SparkSession, functions}
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

  val retweet = ReadAvro.read("data/Retweet","data/Retweet.avsc",spark)

  // UserDir
  WriteJsonToAvro("data/UserDir.json","data/UserDir",spark)

  val userDir = ReadAvro.read("data/UserDir","data/UserDir.avsc",spark)
  userDir.createOrReplaceTempView("users")

  // message
  WriteJsonToAvro("data/Message.json","data/Message",spark)

  val message = ReadAvro.read("data/Message","data/Message.avsc",spark)



  // messageDir
  WriteJsonToAvro("data/MessageDir.json","data/MessageDir",spark)

  val messageDir = ReadAvro.read("data/MessageDir","data/MessageDir.avsc",spark)
  messageDir.createOrReplaceTempView("message")


  val retweetCountSubscriber = retweet.groupBy("USER_ID","MESSAGE_ID").agg(count("SUBSCRIBER_ID").alias("count"))
  retweetCountSubscriber.createOrReplaceTempView("retweetCount")
  retweet.createOrReplaceTempView("retweet")

  spark.sql("select sum(count) as NUMBER_RETWEETS,retweetCount.USER_ID,retweetCount.MESSAGE_ID from " +
    "retweetCount,retweet " +
    "where (SUBSCRIBER_ID == retweetCount.USER_ID AND " +
    "retweet.MESSAGE_ID == retweetCount.MESSAGE_ID ) OR "+
    "(retweet.USER_ID == retweetCount.USER_ID AND " +
    "retweet.MESSAGE_ID == retweetCount.MESSAGE_ID )"+
    "group by retweetCount.USER_ID,retweetCount.MESSAGE_ID " +
    "order by sum(count) DESC").createOrReplaceTempView("simpleTopUser")

  spark.sql("select simpleTopUser.USER_ID,FIRST_NAME,LAST_NAME,simpleTopUser.MESSAGE_ID,message.TEXT,NUMBER_RETWEETS " +
    "from simpleTopUser,users,message " +
    "where message.MESSAGE_ID == simpleTopUser.MESSAGE_ID AND " +
    "users.USER_ID == simpleTopUser.USER_ID " +
    "order by NUMBER_RETWEETS DESC"
  ).show(10)


  /*
  for(r <- retweet) {
    var count = 0
    for(t <- retweetCountSubscriber) {
      if((r.get(0) == t.get(0) && r.get(1) == t.get(1)) ||
        (r.get(1) == t.get(0) && r.get(1) == t.get(1))) count = t.getInt(2) + count
        println(count)
    }
  }

   */


}
