package au.com.nig

import au.com.nig
import org.apache.spark.sql.functions._
import org.scalatest.{Matchers, WordSpec}

class DataframeCreationExperiment extends WordSpec with Matchers {

  val session = TestUtils.createSparkSession()

  "creation of dataframe" should {
    "not be able to run because it cannot infer the schema if nulls and typed attributes and no rows with types" in {

      import session.implicits._
      /**
        * Scala is not infering the type therefore Spark has even less chance of infering it itself
        */

      //GIVEN
      val stuff = Seq(
        (null, null, 1L),
        (1, null, 1L)
      ).toDF("a", "b", "c")
        .withColumn("other_col",
          when(col("a").isNull, map(Seq(col("c"), col("b")): _*))
            .when(col("a").isNotNull, map(Seq(col("a"), col("b")): _*))
            .otherwise(lit(null))
        )

      stuff.count() shouldEqual 2
    }
    "not be able to run because it cannot infer the schema even with options at None" in {

      import session.implicits._
      /**
        * Scala is not infering the type therefore Spark has even less chance of infering it itself
        */

      //GIVEN
      val stuff = Seq(
        (None, None, 1L),
        (Some(1), None, 1L)
      ).toDF("a", "b", "c")
        .withColumn("other_col",
          when(col("a").isNull, map(Seq(col("c"), col("b")): _*))
            .when(col("a").isNotNull, map(Seq(col("a"), col("b")): _*))
            .otherwise(lit(null))
        )

      stuff.count() shouldEqual 2
    }
    "be able to run because it can infer the schema with nulls and no rows with types" in {

      import session.implicits._
      /**
        * Scala is infering the type to Int, Null, Long
        * Spark will be able then to manage it
        */

      //GIVEN
      val stuff = Seq(
        (3, null, 1L),
        (1, null, 1L)
      ).toDF("a", "b", "c")
        .withColumn("other_col",
          when(col("a").isNull, map(Seq(col("c"), col("b")): _*))
            .when(col("a").isNotNull, map(Seq(col("a"), col("b")): _*))
            .otherwise(lit(null))
        )

      stuff.count() shouldEqual 2
    }

    "be able to run because it does not have to infer the schema since it is in the declaration of the sequence" in {

      //GIVEN
      import session.implicits._

      val tuples: Seq[(Integer, String, Long)] = Seq(
        (null, null, 1L),
        (1, null, 1L)
      )

      val stuff = tuples.toDF("a", "b", "c")
        .withColumn("other_col",
          when(col("a").isNull, map(Seq(col("c"), col("b")): _*))
            .when(col("a").isNotNull, map(Seq(col("a"), col("b")): _*))
            .otherwise(lit(null))
        )

      stuff.count() shouldEqual 2
    }
    "be able to run because it does not have to infer the schema since it is in the declaration of the sequence even with options" in {

      //GIVEN
      import session.implicits._

      val tuples: Seq[(Option[Integer], Option[String], Long)] = Seq(
        (None, None, 1L),
        (Some(1), None, 1L)
      )

      val stuff = tuples.toDF("a", "b", "c")
        .withColumn("other_col",
          when(col("a").isNull, map(Seq(col("c"), col("b")): _*))
            .when(col("a").isNotNull, map(Seq(col("a"), col("b")): _*))
            .otherwise(lit(null))
        )

      stuff.count() shouldEqual 2
    }
    "be able to run because it use case classes with types" in {

      //GIVEN
      import session.implicits._

      val tuples: Seq[MyObject] = Seq(
        MyObject(null, null, 1L),
        MyObject(1, null, 1L)
      )

      val stuff = tuples.toDF("A", "B", "C")
        .withColumn("other_col",
          when(col("a").isNull, map(Seq(col("c"), col("b")): _*))
            .when(col("a").isNotNull, map(Seq(col("a"), col("b")): _*))
            .otherwise(lit(null))
        )

      stuff.count() shouldEqual 2
    }
  }

}

case class MyObject(A: Integer,
                    B: Integer,
                    C: Long)
