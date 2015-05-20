package com.stackoverflow.example

import org.apache.spark.storage.StorageLevel
import org.apache.spark.{SparkConf, SparkContext}

/**
 * Created by Alvin on 5/20/15.
 */

case class User(displayName: String, reputation: Int)

object UserCount {

  def main(args: Array[String]) {
    val inputFile = args(0)
    //val outputFile = args(1)

    val Regex = """^.*Reputation="([0-9]+)" CreationDate="([0-9]{4}-[0-9]{2}-[0-9]{2})T.*" DisplayName="(.+)" LastAccessDate="(.+)" .*$""".r

    def processRow(row: String) = row match {
      case Regex(reputation, creationDate, displayName, lastAccess) => Some(User(displayName, reputation.toInt))
      case _ => None
    }



    val conf = new SparkConf().setAppName("Stack overflow user count").set("spark.executor.memory", "1g")
    val sc = new SparkContext(conf)

    val allUsers = sc.textFile(inputFile).persist(StorageLevel.DISK_ONLY)

    val top5 = allUsers.map(processRow).collect {
      case Some(user) => user.reputation -> user.displayName
    }.sortByKey(ascending = false).take(5)


    top5.foreach {
      case (reputation, displayName) => println("%s ===>>> %d points".format(displayName, reputation))
    }
    //counts.saveAsTextFile(outputFile)
  }

}
