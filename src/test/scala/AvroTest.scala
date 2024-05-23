import org.apache.spark.sql.SparkSession
import org.scalatest.funsuite.AnyFunSuite

import java.io.File

class AvroTest extends AnyFunSuite{
  test("Start test 2 + 2") {
    val test = 2 + 2
    assert(test == 4)
    assert(test != 6)
  }

  val spark = SparkSession.builder()
    .appName("AvroTest")
    .master("local[*]")
    .getOrCreate()

  test("write an avro file") {
    WriteJsonToAvro("data/test/test.json","data/test/test",spark)

    assert(new File("data/test/test").exists())
    assert(new File("data/test/test/_SUCCESS").exists())
  }

  test("read an avro file") {
    WriteJsonToAvro("data/test/test.json","data/test/test",spark)

    val test = ReadAvro.read("data/test/test","data/test/test.avsc",spark)

    assert(!test.isEmpty)
    assert(test.count() == 1)
    assert(test.schema.nonEmpty)
    assert(test.schema.names.sameElements(Array("id","test")))

  }



}
