package com.udf.exampe

import com.util.UDFUtil.UDFContext
import org.apache.spark.SparkContext
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.functions._

/**
  * Created by shona on 12/17/15.
  */
object UDFTest {
  def main(args: Array[String]) {
    val master = args.length match {
      case x: Int if x > 0 => args(0)
      case _ => "local"
    }
    val sc = new SparkContext(master, "BasicAvg", System.getenv("SPARK_HOME"))

    implicit val sqlContext = new SQLContext(sc)
    import sqlContext.implicits._

    val udfContext = new UDFContext()
    import udfContext._

    //sqlContext.udf.register("func2", func2)

    val yy = (i: String) => Option(i).map(x => x.toInt + 1)

    val func2 = udf(yy)

    val df = sqlContext.read.json("/Users/shona/IdeaProjects/apache-spark-examples/data/people.json")

    df.select(func2($"age") as "added1").show

    df.select(callUDF("func3", $"age") as "added1").show

    sc.stop()
  }

}
