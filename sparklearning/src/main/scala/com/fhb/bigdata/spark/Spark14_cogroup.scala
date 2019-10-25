package com.fhb.bigdata.spark

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * Created by IntelliJ IDEA 2019.2.3.
 * User: fhb
 * Email: fhb7218@gmail.com
 * Date: 2019/10/25
 * Time: 17:41
 *
 * @author hbfang
 */

object Spark14_cogroup {
  def main(args: Array[String]): Unit = {
    val config: SparkConf = new SparkConf().setMaster("local").setAppName("Op4")

    val sc = new SparkContext(config)

    val rdd1: RDD[(Int, String)] = sc.makeRDD(Array((1, "a"), (2, "b"), (4, "c")))
    val rdd2: RDD[(Int, String)] = sc.makeRDD(Array((1, "aa"), (2, "bb"), (3, "cc")))


    rdd1.cogroup(rdd2).collect().foreach(println)
  }
}
