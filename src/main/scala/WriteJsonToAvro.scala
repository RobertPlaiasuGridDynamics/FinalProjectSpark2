import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.types.{IntegerType, LongType, StructField}

import java.io.File
import scala.reflect.io.Path.jfile2path


object WriteJsonToAvro {
  def apply(jsonFilePath : String,avroFilePath : String,spark : SparkSession) : Unit = {
    if(!new File(avroFilePath).exists()) {
      // Load JSON data into a DataFrame
      val df = spark
        .read
        .option("multiLine", true)
        .json(jsonFilePath)

      val cols = df.schema.map {
        case StructField(name, LongType, _, _) => col(name).cast(IntegerType)
        case f => col(f.name)
      }



      // Write the DataFrame to an Avro file
      df
        .select(cols : _*)
        .write
        .format("avro")
        .save(avroFilePath)
    }
  }
}
