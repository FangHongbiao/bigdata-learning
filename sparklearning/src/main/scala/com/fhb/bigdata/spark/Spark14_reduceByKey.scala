package com.fhb.bigdata.spark

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
;

/**
 * Created by IntelliJ IDEA 2019.2.3.
 * User: fhb
 * Email: fhb7218@gmail.com
 * Date: 2019/10/25
 * Time: 15:28
 *
 * @author hbfang
 */

/**
 * reduceByKey 算子
 */
object Spark14_reduceByKey {
  def main(args: Array[String]): Unit = {
    val config: SparkConf = new SparkConf().setMaster("local").setAppName("Op4")

    val sc = new SparkContext(config)

    val listRDD: RDD[String] = sc.makeRDD(List("one", "two", "one", "three", "two", "two", "five"))

    val mapRDD: RDD[(String, Int)] = listRDD.map((_, 1))

    val reduceByKeyRDD: RDD[(String, Int)] = mapRDD.reduceByKey(_+_)

    reduceByKeyRDD.collect().foreach(println)
  }
}
