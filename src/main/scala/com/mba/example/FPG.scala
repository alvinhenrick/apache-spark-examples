package com.mba.example

import org.apache.spark.SparkContext
import org.apache.spark.mllib.fpm.FPGrowth

/**
 * Created by shona on 7/28/15.
 */
object FPG {

  def main(args: Array[String]) {
    val master = args.length match {
      case x: Int if x > 0 => args(0)
      case _ => "local"
    }
    val sc = new SparkContext(master, "MBA", System.getenv("SPARK_HOME"))

    val transactions = sc.textFile("/Users/shona/IdeaProjects/apache-spark-examples/data/transactions", 1)

    val fpgformat = transactions.map(x => x.split("[,]"))

    val fpg = new FPGrowth()
      .setMinSupport(.1)
      .setNumPartitions(1)

    val model = fpg.run(fpgformat)


    model.freqItemsets.collect().foreach { itemset =>
      println(itemset.items.mkString("[", ",", "]") + ", " + itemset.freq)
    }



  }

}
