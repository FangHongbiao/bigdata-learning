package com.fhb.bigdata.spark

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * Created by IntelliJ IDEA 2019.2.3.
 * User: fhb
 * Email: fhb7218@gmail.com
 * Date: 2019/10/25
 * Time: 10:34
 *
 * @author hbfang
 */

/**
 * coalesce 算子
 */
object Spark11_Oper10 {
  def main(args: Array[String]): Unit = {
    val config: SparkConf = new SparkConf().setMaster("local").setAppName("Op4")

    val sc = new SparkContext(config)

    val listRDD: RDD[Int] = sc.makeRDD(1 to 18, 4)

    println(listRDD.partitions.size)

    val coalesceRDD: RDD[Int] = listRDD.coalesce(3)

    println(coalesceRDD.partitions.size)

  }
}
