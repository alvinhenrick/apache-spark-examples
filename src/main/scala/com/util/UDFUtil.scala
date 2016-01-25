package com.util

import org.apache.spark.sql.SQLContext

/**
  * Created by shona on 1/6/16.
  */
object UDFUtil {

  val func = (i: Any) => Option(i).map(x => x.toString.toInt + 1)

  class UDFContext(implicit sqlContext: SQLContext) {
    val func3 = sqlContext.udf.register("func3", func)

  }

}


