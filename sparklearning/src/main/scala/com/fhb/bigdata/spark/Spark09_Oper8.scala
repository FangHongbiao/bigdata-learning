package com.fhb.bigdata.spark

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * Created by IntelliJ IDEA 2019.2.3.
 * User: fhb
 * Email: fhb7218@gmail.com
 * Date: 2019/10/25
 * Time: 10:13
 *
 * @author hbfang
 */

/**
 * sample 算子
 */
object Spark09_Oper8 {
  def main(args: Array[String]): Unit = {
    val config: SparkConf = new SparkConf().setMaster("local").setAppName("Op4")

    val sc = new SparkContext(config)

    val listRDD: RDD[Int] = sc.makeRDD(1 to 10)

    val sampleRDD: RDD[Int] = listRDD.sample(false, 0.4, 1)

    sampleRDD.collect().foreach(println)

  }
}
