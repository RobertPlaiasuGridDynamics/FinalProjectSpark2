
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.count
import org.scalatest.funsuite.AnyFunSuite

class RetweetTest extends AnyFunSuite {
  val spark = SparkSession.builder()
    .appName("AvroTest")
    .master("local[*]")
    .getOrCreate()


  test("count retweets") {
    WriteJsonToAvro("data/Retweet.json","data/Retweet",spark)

    val retweet = ReadAvro.read("data/Retweet","data/Retweet.avsc",spark)
    val retweetCountSubscriber = retweet.groupBy("USER_ID","MESSAGE_ID").agg(count("SUBSCRIBER_ID").alias("count"))
    assert(!retweetCountSubscriber.isEmpty)

    assert(retweetCountSubscriber.columns.length == 3)
    assert(retweetCountSubscriber.columns.sameElements(Array("USER_ID","MESSAGE_ID","count")))
  }


}
