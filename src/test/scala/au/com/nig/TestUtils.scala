package au.com.nig

import java.io.File

import org.apache.commons.io.FileUtils
import org.apache.spark.sql.{DataFrame, SparkSession}

object TestUtils {

  def createSparkSession(appName: String = getClass.getName): SparkSession = {
    SparkSession.clearActiveSession()
    val globalSession = SparkSession.getDefaultSession.getOrElse(SparkSession.builder()
      .appName("Global session")
      .master("local[*]")
      .config("avro.mapred.ignore.inputs.without.extension", value = false)
      .config("spark.debug.maxToStringFields", 5000)
      .getOrCreate())
    val session = globalSession.newSession()
    session.conf.set("spark.app.name", appName)
    SparkSession.setActiveSession(session)
    session
  }

  /**
   * Returns the session associated to the current thread. Throws an exception if there is no session.
   */
  def activeSession: SparkSession = SparkSession.getActiveSession.get

  def delete(folder: String): Unit = FileUtils.deleteDirectory(new File(folder))

  def createTestDataframe(implicit session: SparkSession): DataFrame = {
    import session.implicits._
    val cols = Seq("colA", "colB", "colC")
    val rows = Seq(
      ("testA0", "testB0", "testC0"),
      ("testA0", "testB0", "testC0"))
    rows.toDF(cols: _*)
  }

}

trait TestSupport {
  def testFolderPath(folderName: String): String = s"target/test_folders/${getClass.getName}/$folderName"

}