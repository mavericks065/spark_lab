package au.com.nig.s3io

import java.io._
import java.nio.charset.StandardCharsets.UTF_8
import java.util.zip.ZipInputStream

import org.apache.commons.io.IOUtils
import org.slf4j.LoggerFactory

object S3Unzip {
  private val log = LoggerFactory.getLogger(getClass)

  def localUnZipIt(zipFile: String, outputFolder: String): Seq[String] = {
    val folder = new File(outputFolder)
    if (!folder.exists())
      folder.mkdirs()
    log.debug(s"Unzipping file $zipFile")
    val is = new FileInputStream(zipFile)
    unZipIt(
      is,
      (fileName, _, zis) => {
        val newFile = new File(outputFolder + "/" + fileName)
        val out = new FileOutputStream(newFile)
        try {
          IOUtils.copy(zis, out)
        } finally
          out.close()
      }).map(outputFolder + "/" + _)
  }

  def unZipIt(inputBucket: String, zipInputPath: String, outputBucket: String, outputPath: String): Seq[String] = {
    log.debug(s"Unzipping file $inputBucket $zipInputPath")
    val is = S3Io.readFromS3(inputBucket, zipInputPath)
    unZipIt(
      is,
      (fileName, fileLength, zis) => {
        val key = outputPath + fileName
        S3Io.writeInS3(outputBucket, key, zis, fileLength)
      })
      .map(fileName => s"s3://$outputBucket/$outputPath$fileName")
  }

  private def unZipIt(rawInputStream: InputStream, fn: (String, Long, InputStream) => Unit): Seq[String] = {
    val zis = new ZipInputStream(rawInputStream, UTF_8)
    try {
      val wrappedInputStream = new UnCloseableInputStream(zis)
      val iterator = Iterator.continually(Option(zis.getNextEntry)).takeWhile(_.isDefined).flatten
      val fileNames = for (ze <- iterator) yield {
        val fileName = ze.getName
        log.debug(s"Found zip entry $fileName (${ze.getSize} Bytes)")
        fn(fileName, ze.getSize, wrappedInputStream)
        fileName
      }
      fileNames.toList // toList triggers the evaluation
    } finally
      zis.close()
  }

  class UnCloseableInputStream(inputStream: InputStream) extends FilterInputStream(inputStream) {
    override def close(): Unit = {}
  }
}
