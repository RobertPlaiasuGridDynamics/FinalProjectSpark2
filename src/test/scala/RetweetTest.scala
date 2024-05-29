
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, count}
import org.scalatest.funsuite.AnyFunSuite

class RetweetTest extends AnyFunSuite {
  val spark = SparkSession.builder()
    .appName("AvroTest")
    .master("local[*]")
    .getOrCreate()


  test("read from avro retweets") {
    WriteJsonToAvro("data/Retweet.json","data/Retweet",spark)

    val retweet = ReadAvro.read("data/Retweet","data/Retweet.avsc",spark)

    assert(!retweet.isEmpty)

    assert(retweet.columns.sameElements(Array("USER_ID","MESSAGE_ID","SUBSCRIBER_ID")))


    import spark.implicits._
    assert(retweet
      .select(col("USER_ID"),col("SUBSCRIBER_ID"),col("MESSAGE_ID"))
      .as[(Int,Int,Int)]
      .collect()
      .toList == List((1,2,11),(1,3,11),(2,5,11),(3,7,11),(7,14,11),(5,33,11),(2,4,12),(3,8,13))
    )
  }

  test("group retweets by user_id and message_id and count them") {
    WriteJsonToAvro("data/Retweet.json","data/Retweet",spark)

    val retweet = ReadAvro.read("data/Retweet","data/Retweet.avsc",spark)
    val retweetCountSubscriber = retweet.groupBy("USER_ID","MESSAGE_ID").agg(count("SUBSCRIBER_ID").alias("count"))

    assert(!retweetCountSubscriber.isEmpty)
    assert(retweetCountSubscriber.columns.sameElements(Array("USER_ID","MESSAGE_ID","count")))

    import spark.implicits._
    assert(retweetCountSubscriber
      .select(col("USER_ID"),col("count"),col("MESSAGE_ID"))
      .as[(Int,BigInt,Int)]
      .collect()
      .toList == List((3,1,13),(1,2,11),(7,1,11),(3,1,11),(2,1,11),(2,1,12),(5,1,11))
    )
  }


}
