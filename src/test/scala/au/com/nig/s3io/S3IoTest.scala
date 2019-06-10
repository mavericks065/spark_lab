package au.com.nig.s3io

import au.com.nig.s3io.S3Io._
import com.amazonaws.services.s3.model.S3ObjectSummary
import org.scalatest.{Matchers, WordSpec}

import scala.concurrent.Future

class S3IoTest extends WordSpec with Matchers {
  def s3Object(key: String): S3ObjectSummary = {
    val obj = new S3ObjectSummary()
    obj.setKey(key)
    obj
  }

  def countFolders(bucket: String, prefix: String): Future[Int] = {
    parallelBrowseFolder(
      bucket,
      prefix,
      process = {
        case FoundS3Folder(_) =>
          Seq(1)
        case event @ BrowseError(_, _, _) =>
          Seq()
      }).map(_.sum)
  }

  "iterate" should {
    "return an iterator with all the values produced" in {
      val result = S3Io.iterate[Int] {
        case None => Some(0)
        case Some(i) if i < 5 => Some(i + 1)
        case _ => None
      }
      result.toSeq shouldEqual Seq(0, 1, 2, 3, 4, 5)
    }
    "work with an empty result" in {
      val result = S3Io.iterate[Int](_ => None)
      result.toSeq shouldEqual Seq()
    }
  }

  "folder" should {
    "extract the folder from a S3 object" in {
      val obj = s3Object("folder1/folder2/file.txt")
      S3Io.folder(obj) shouldEqual "folder1/folder2/"
    }
  }

  "groupByFolder" should {
    "group S3 objects by folder" in {
      val files = Iterator(
        s3Object("folder1/folder2/file1.txt"),
        s3Object("folder1/folder2/file2.txt"),
        s3Object("folder1/folder3/file3.txt"),
        s3Object("folder1/folder4/file4.txt"))
      val folders = S3Io.groupByFolder(files)
      val result = folders.map(folder => s"${folder.path}\n${folder.files.map("   " + _.getKey).mkString("\n")}").mkString("\n")
      val expected =
        """folder1/folder2/
          |   folder1/folder2/file1.txt
          |   folder1/folder2/file2.txt
          |folder1/folder3/
          |   folder1/folder3/file3.txt
          |folder1/folder4/
          |   folder1/folder4/file4.txt""".stripMargin
      result shouldEqual expected
    }
  }
}
