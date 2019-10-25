package com.fhb.bigdata.spark

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * Created by IntelliJ IDEA 2019.2.3.
 * User: fhb
 * Email: fhb7218@gmail.com
 * Date: 2019/10/25
 * Time: 10:25
 *
 * @author hbfang
 */

/**
 * distinct 算子
 */
object Spark10_Oper9 {
  def main(args: Array[String]): Unit = {
    val config: SparkConf = new SparkConf().setMaster("local").setAppName("Op4")

    val sc = new SparkContext(config)

    val listRDD: RDD[Int] = sc.makeRDD(List(1,2,2,2,2,3,3,4,54,6,9))

    val distinctRDD: RDD[Int] = listRDD.distinct(2)

    distinctRDD.saveAsTextFile("out")
  }
}
