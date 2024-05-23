
import org.apache.avro.Schema
import org.apache.spark.sql.{DataFrame, SparkSession}

import java.io.File

object ReadAvro {
  def read(avroPathFile : String,schemaPathFile : String,spark : SparkSession): DataFrame = {
    val schema = new Schema
    .Parser()
      .parse(new File(schemaPathFile))

    spark.read
      .format("avro")
      .option("avroSchema", schema.toString)
      .load(avroPathFile)
  }


}
