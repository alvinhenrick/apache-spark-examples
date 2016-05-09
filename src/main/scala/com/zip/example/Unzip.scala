package com.zip.example

import com.cotdp.hadoop.ZipFileInputFormat
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileStatus, FileSystem, Path}
import org.apache.hadoop.io.{BytesWritable, Text}
import org.apache.spark.{SparkConf, SparkContext}


/**
  * Created by Alvin on 12/12/15.
  */
object ProcessFile extends Serializable {

  def apply(fileName: String, records: BytesWritable): Unit = {
    val hadoopConf = new Configuration()
    val fs = FileSystem.get(hadoopConf)
    if (records.getLength > 0) {
      val outFileStream = fs.create(new Path("/Users/shona/IdeaProjects/apache-spark-examples/data/temp/" + fileName), true)
      outFileStream.write(records.getBytes)
      outFileStream.close()
    }
  }
}

object Unzip {

  def main(args: Array[String]) {
    val master = args.length match {
      case x: Int if x > 0 => args(0)
      case _ => "local[*]"
    }
    val hadoopConf = new Configuration()
    val fs = FileSystem.get(hadoopConf)

    val sparkConf = new SparkConf()
    sparkConf.setMaster(master)
    sparkConf.setAppName("Unzip")
    sparkConf.setSparkHome(System.getenv("SPARK_HOME"))

    val sc = new SparkContext(sparkConf)

    val fileSystem = listLeafStatuses(fs, new Path("/Users/shona/IdeaProjects/apache-spark-examples/data/test"))

    val allzip = fileSystem.filter(_.getPath.getName.endsWith("zip"))

    allzip.foreach { x =>
      val zipFileRDD = sc.newAPIHadoopFile(
        x.getPath.toString,
        classOf[ZipFileInputFormat],
        classOf[Text],
        classOf[BytesWritable], hadoopConf)

      zipFileRDD.foreach { y =>
        ProcessFile(y._1.toString, y._2)
      }
    }

  }

  /**
    * Get [[org.apache.hadoop.fs.FileStatus]] objects for all leaf children (files) under the given base path. If the
    * given path points to a file, return a single-element collection containing [[org.apache.hadoop.fs.FileStatus]] of
    * that file.
    */
  def listLeafStatuses(fs: FileSystem, basePath: Path): Seq[FileStatus] = {
    listLeafStatuses(fs, fs.getFileStatus(basePath))
  }

  /**
    * Get [[FileStatus]] objects for all leaf children (files) under the given base path. If the
    * given path points to a file, return a single-element collection containing [[FileStatus]] of
    * that file.
    */
  def listLeafStatuses(fs: FileSystem, baseStatus: FileStatus): Seq[FileStatus] = {
    def recurse(status: FileStatus): Seq[FileStatus] = {
      val (directories, leaves) = fs.listStatus(status.getPath).partition(_.isDirectory)
      leaves ++ directories.flatMap(f => listLeafStatuses(fs, f))
    }

    if (baseStatus.isDirectory) recurse(baseStatus) else Seq(baseStatus)
  }

}






