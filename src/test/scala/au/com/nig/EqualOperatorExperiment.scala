package au.com.nig

import org.apache.spark.sql.functions.lit
import org.apache.spark.sql.types.StringType
import org.apache.spark.sql.{AnalysisException, SparkSession}
import org.scalatest.{Matchers, WordSpec}

class EqualOperatorExperiment extends WordSpec with Matchers {

  "operator <=>" should {
    "be safe if values are null" in {
      implicit val session: SparkSession = TestUtils.createSparkSession()
      import session.implicits._


      val df = Seq(("123", "foo", "456"), ("321", "bar", "654"))
        .toDF("col1", "col2", "col3")

      val df2 = session.emptyDataFrame
        .withColumn("col1", lit(null).cast(StringType))
        .withColumn("col_2", lit("bluh").cast(StringType))
        .withColumn("col_3", lit("bla").cast(StringType))

      val df3 = Seq((null, "foo", "456"), ("321", "bar", "654"))
        .toDF("col1", "col2", "col3")

      // WHEN
      val result = df.join(df2, df("col1") <=> df2("col1"), "left")

      // THEN

      result.explain()

      result.show()
      result.count() shouldEqual 2
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
      val msg = intercept[AnalysisException] {

        df.join(df2, df("col1") === df2("col1"), "left")
      }

      // THEN
      assert(msg.getMessage() contains "Cannot resolve column name \"col1\" among (col2, col3);")

    }
  }
  "operator ===" should {
    "be safe if values are null" in {
      implicit val session: SparkSession = TestUtils.createSparkSession()
      import session.implicits._


      val df = Seq((null, "foo", "456"), ("321", "bar", "654"))
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
      val msg = intercept[AnalysisException] {

        df.join(df2, df("col1") === df2("col1"), "left")
      }

      // THEN
      assert(msg.getMessage() contains "Cannot resolve column name \"col1\" among (col2, col3);")
    }
  }

}
