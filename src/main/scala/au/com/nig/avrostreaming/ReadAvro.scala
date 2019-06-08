package au.com.nig.avrostreaming

import org.apache.spark.streaming.StreamingContext
import org.apache.avro.mapred.{AvroKey, AvroValue}
import org.apache.avro.mapreduce.AvroKeyValueInputFormat
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.StructType

sealed trait StreamingInput

case class StreamingFileInput(path: String) extends StreamingInput

object ReadAvro {

  def readAvro(ssc: StreamingContext, input: StreamingFileInput, strucType: StructType)(implicit session: SparkSession) = {
    session.readStream.format("avro").schema(strucType).load(input.path)
  }

}
