package com.zip.example

import com.cotdp.hadoop.ZipFileInputFormat
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
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
      val outFileStream = fs.create(new Path("/Users/shona/IdeaProjects/apache-spark-examples/data/" + fileName), true)
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

    val sparkConf = new SparkConf()
    sparkConf.setMaster(master)
    sparkConf.setAppName("Unzip")
    sparkConf.setSparkHome(System.getenv("SPARK_HOME"))

    val sc = new SparkContext(sparkConf)

    val zipFileRDD = sc.newAPIHadoopFile(
      "/Users/shona/IdeaProjects/apache-spark-examples/data/test",
      classOf[ZipFileInputFormat],
      classOf[Text],
      classOf[BytesWritable], hadoopConf)

    zipFileRDD.foreach { x =>
      ProcessFile(x._1.toString, x._2)
    }

  }
}






