package com.fhb.bigdata.spark

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * Created by IntelliJ IDEA 2019.2.3.
 * User: fhb
 * Email: fhb7218@gmail.com
 * Date: 2019/10/25
 * Time: 8:47
 *
 * @author hbfang
 */

/**
 * 测试 flatMap 算子
 */
object Spark06_Oper5 {
  def main(args: Array[String]): Unit = {
    val config: SparkConf = new SparkConf().setMaster("local").setAppName("Op4")

    val sc = new SparkContext(config)

    val listRDD: RDD[Int] = sc.makeRDD(1 to 18, 4)

    val glomRDD: RDD[Array[Int]] = listRDD.glom()

    glomRDD.collect().foreach(arr => println(arr.mkString(",")))
  }
}
