package au.com.nig

import org.apache.spark.sql.SparkSession
import org.scalatest.{Matchers, WordSpec}
import org.apache.spark.sql.functions.{col, lit}
import org.apache.spark.sql.types.StringType

class Experiment extends WordSpec with Matchers {

  "operator <=>" should {
    "be safe if values are null" in {
      implicit val session: SparkSession = TestUtils.createSparkSession()
      import session.implicits._


      val df = Seq(("123", "foo", "456"), ("321", "bar", "654"))
        .toDF("col1", "col2", "col3")

      val df2 = session.emptyDataFrame
        .withColumn("col11", lit(null).cast(StringType))
        .withColumn("col2", lit(null).cast(StringType))
        .withColumn("col3", lit(null).cast(StringType))


      // WHEN
      val result = df.join(df2, df("col1") <=> df2("col11"), "left")

      // THEN

      result.count() shouldEqual 2
      result.show()
    }
    "be safe if columns are null" in {

      implicit val session: SparkSession = TestUtils.createSparkSession()
      import session.implicits._


      val df = Seq(("123", "foo", "456"), ("321", "bar", "654"))
        .toDF("col1", "col2", "col3")

      val df2 = session.emptyDataFrame
        .withColumn("col2", lit(null).cast(StringType))
        .withColumn("col3", lit(null).cast(StringType))


      // WHEN
      val result = df.join(df2, df("col1") <=> df2("col1"), "left")

      // THEN

      result.count() shouldEqual 2
      result.show()
    }
  }
  "operator ===" should {
    "be safe if values are null" in {
      implicit val session: SparkSession = TestUtils.createSparkSession()
      import session.implicits._


      val df = Seq(("123", "foo", "456"), ("321", "bar", "654"))
        .toDF("col1", "col2", "col3")

      val df2 = session.emptyDataFrame
        .withColumn("col1", lit(null).cast(StringType))
        .withColumn("col2", lit(null).cast(StringType))
        .withColumn("col3", lit(null).cast(StringType))


      // WHEN
      val result = df.join(df2, df("col1") === df2("col1"), "left")

      // THEN

      result.count() shouldEqual 2
      result.show()
    }
    "not be safe if columns are null" in {

      implicit val session: SparkSession = TestUtils.createSparkSession()
      import session.implicits._


      val df = Seq(("123", "foo", "456"), ("321", "bar", "654"))
        .toDF("col1", "col2", "col3")

      val df2 = session.emptyDataFrame
        .withColumn("col2", lit(null).cast(StringType))
        .withColumn("col3", lit(null).cast(StringType))


      // WHEN
      val result = df.join(df2, df("col1") === df2("col1"), "left")

      // THEN

      result.count() shouldEqual 2
      result.show()
    }
  }

}
