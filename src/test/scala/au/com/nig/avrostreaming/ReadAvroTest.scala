package au.com.nig.avrostreaming

import java.nio.file.{Files, Paths, StandardCopyOption}

import au.com.nig.TestUtils
import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming.{Milliseconds, StreamingContext}
import org.scalatest.WordSpec


class ReadAvroTest extends WordSpec {


  "read fn" should {
    "return a DStream with the content of the AVRO files in the case of AVROs" in {
      // GIVEN
      implicit val session: SparkSession = TestUtils.createSparkSession("test")
      val ssc = new StreamingContext(session.sparkContext, Milliseconds(500))

      val input = StreamingFileInput("src/test/resources/streaming/avroInput")
      val df = session.read.format("avro").load("src/test/resources/streaming/avro/rm_1.avro")
      df.show()
      val streamingDf = ReadAvro.readAvro(ssc, input, df.schema)

      streamingDf.printSchema()
      val stream = streamingDf.writeStream
      stream.foreachBatch((ds, l) => ds.show())

      stream.start()


      val src = "src/test/resources/streaming/avro/rm_1.avro"
      val src2 = "src/test/resources/streaming/avro/rm_2.avro"
      val dest = "src/test/resources/streaming/avroInput/rm_1.avro"
      val dest2 = "src/test/resources/streaming/avroInput/rm_2.avro"

      Thread.sleep(1500)

      Files.copy(
        Paths.get(src),
        Paths.get(dest),
        StandardCopyOption.REPLACE_EXISTING)

      Thread.sleep(500)

      Files.copy(
        Paths.get(src2),
        Paths.get(dest2),
        StandardCopyOption.REPLACE_EXISTING)

      Thread.sleep(5000)

      // THEN
    }
  }

}
