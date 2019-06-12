package au.com.nig.s3io

import java.io.InputStream
import java.util.concurrent.Executors.newFixedThreadPool

import com.amazonaws.ClientConfiguration
import com.amazonaws.regions.Regions.AP_SOUTHEAST_2
import com.amazonaws.services.s3.{AmazonS3, AmazonS3ClientBuilder}
import com.amazonaws.services.s3.model._
import com.amazonaws.services.s3.transfer.TransferManagerBuilder

import scala.concurrent.ExecutionContext.fromExecutor
import scala.concurrent.{ExecutionContextExecutor, Future}
import scala.util.Try
import scala.collection.JavaConversions._

case class S3Folder(bucket: String, path: String, files: Seq[S3ObjectSummary], subFolders: Seq[String] = Seq())

sealed trait BrowseEvent

case class FoundS3Folder(folder: S3Folder) extends BrowseEvent

case class BrowseError(bucket: String, prefix: String, t: Throwable) extends BrowseEvent

object S3Path {
  def unapply(str: String): Option[(String, String, Option[String])] = {
    if (str.startsWith("s3://")) {
      val tokens = str.substring(5).split("/")
      val bucket = tokens(0)
      val key = tokens.tail.mkString("/")
      val table = tableName(str)
      Some(bucket, key, table)
    } else
      None
  }
  def tableName(path: String): Option[String] = path
    .split("/")
    .filterNot(_.contains("=")) // remove partitions by column
    .filter(_.matches(".*[a-zA-Z].*")) // require an alpha character, remove date partitions
    .lastOption
}
object S3Io {
  val clientConfiguration = new ClientConfiguration()
  val s3Client: AmazonS3 = AmazonS3ClientBuilder.standard()
    .withRegion(AP_SOUTHEAST_2)
    .withClientConfiguration(clientConfiguration)
    .build()

  implicit val executor: ExecutionContextExecutor = fromExecutor(
    newFixedThreadPool(100))

  def readFromS3(uri: String): InputStream = uri match {
    case S3Path(bucket, prefix, _) => readFromS3(bucket, prefix)
  }

  def readFromS3(bucket: String, prefix: String): InputStream = {
    s3Client
      .getObject(new GetObjectRequest(bucket, prefix))
      .getObjectContent
  }

  def writeInS3(bucket: String, key: String, inputStream: InputStream, contentLength: Long): Unit = {
    val objectMetadata = new ObjectMetadata()
    objectMetadata.setContentLength(contentLength)
    val s3TransferManager = TransferManagerBuilder.standard().withS3Client(s3Client).build()
    s3TransferManager
      .upload(bucket, key, inputStream, objectMetadata)
      .waitForUploadResult()
    s3TransferManager.shutdownNow(false)
  }
  /**
    * Creates an Iterator from a function that generates objects.
    * - the function receives the previous object as a parameter or None for the first call
    * - the function should return Some(object) or None when finished
    */
  private[s3io] def iterate[T](fn: Option[T] => Option[T]): Iterator[T] = Iterator
    .iterate[Option[T]](None)(fn)
    .drop(1) // the first element was None
    .takeWhile(_.isDefined)
    .flatten

  def listS3Objects(uri: String): Iterator[S3ObjectSummary] = uri match {
    case S3Path(bucket, prefix, _) => listS3Objects(bucket, prefix)
  }

  def listS3Objects(bucket: String, prefix: String): Iterator[S3ObjectSummary] = {

    def newRequest = new ListObjectsV2Request()
      .withBucketName(bucket)
      .withPrefix(prefix)

    def execute(request: ListObjectsV2Request) = {
      val response = s3Client.listObjectsV2(request)
      response
    }

    iterate[ListObjectsV2Result] {
      case None => // first request
        Some(execute(newRequest))
      case Some(response) if response.isTruncated => // paging
        Some(execute(newRequest.withContinuationToken(response.getNextContinuationToken)))
      case _ => // finished
        None
    }.flatMap(_.getObjectSummaries.toList) // toList to force conversion to a scala list so that IntelliJ is happy
  }

  def listS3Folder(bucket: String, path: String): S3Folder = {
    def newRequest = new ListObjectsV2Request()
      .withBucketName(bucket)
      .withPrefix(path)
      .withDelimiter("/")

    def execute(request: ListObjectsV2Request) = {
      val response = s3Client.listObjectsV2(request)
      response
    }

    var response = execute(newRequest)
    var files: Seq[S3ObjectSummary] = response.getObjectSummaries
    var subfolders: Seq[String] = response.getCommonPrefixes

    while (response.isTruncated) {
      response = execute(newRequest.withContinuationToken(response.getNextContinuationToken))
      files = files ++ response.getObjectSummaries
      subfolders = subfolders ++ response.getCommonPrefixes
    }
    S3Folder(bucket, path, files, subfolders)
  }

  def parallelBrowseFolder[R](bucket: String, prefix: String, process: BrowseEvent => Seq[R]): Future[Seq[R]] = {
    Future {
      Try(listS3Folder(bucket, prefix))
        .map(FoundS3Folder)
        .recover { case t => BrowseError(bucket, prefix, t) }
        .get
    }.flatMap { event =>
      val result = process(event)
      event match {
        case FoundS3Folder(f) =>
          val subFoldersResults = Future.traverse(f.subFolders)(sf => parallelBrowseFolder(bucket, sf, process))
          subFoldersResults.map(result ++ _.flatten)
        case _ =>
          Future(result)
      }
    }
  }

  private[s3io] def folder(file: S3ObjectSummary): String = {
    val path = file.getKey
    path.lastIndexOf('/') match {
      case -1 => path
      case i => path.substring(0, i + 1)
    }
  }

  private[s3io] def groupByFolder(files: Iterator[S3ObjectSummary]): Iterator[S3Folder] = {
    var remainingFiles = files
    Iterator
      .continually {
        if (remainingFiles.hasNext) {
          val file = remainingFiles.next()
          val currentBucket = file.getBucketName
          val currentPath = folder(file)
          val (head, tail) = remainingFiles.span(f => f.getBucketName == currentBucket && folder(f) == currentPath)
          remainingFiles = tail
          Some(S3Folder(currentBucket, currentPath, Seq(file) ++ head))
        } else {
          None
        }
      }
      .takeWhile(_.isDefined)
      .flatten
  }
}
