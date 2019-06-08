package au.com.nig.s3io

import java.io.File

import org.apache.commons.io.FileUtils
import org.scalatest.{FlatSpec, Matchers}

class S3UnzipTest extends FlatSpec with Matchers {
  def testFolder(folderName: String): String = s"target/test_folders/${getClass.getName}/$folderName"

  "Unzip" should "unzip files" in {
    // GIVEN
    val folder = testFolder("zip")
    FileUtils.deleteDirectory(new File(folder))
    new File(s"$folder/test1.txt").exists() shouldBe false
    //WHEN
    val result = S3Unzip.localUnZipIt("src/test/resources/test.zip", folder)

    //THEN
    result shouldEqual Seq(s"$folder/test1.txt", s"$folder/test2.txt")
    new File(s"$folder/test1.txt").exists() shouldBe true
    new File(s"$folder/test2.txt").exists() shouldBe true
    new File(s"$folder/test3.txt").exists() shouldBe false
  }

}