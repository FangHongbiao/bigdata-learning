package com.fhb.bigdata.spark

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD

/**
 * Created by IntelliJ IDEA 2019.2.3.
 * User: fhb
 * Email: fhb7218@gmail.com
 * Date: 2019/10/25
 * Time: 14:42
 *
 * @author hbfang
 */

/**
 * repartition 算子
 */
object Spark12_Oper11 {
  def main(args: Array[String]): Unit = {
    val config: SparkConf = new SparkConf().setMaster("local").setAppName("Op4")

    val sc = new SparkContext(config)

    val listRDD: RDD[Int] = sc.makeRDD(1 to 18, 4)

    listRDD.glom().collect().foreach(arr => println(arr.mkString(",")))

    val repartitionRDD: RDD[Int] = listRDD.repartition(3)

    repartitionRDD.glom().collect().foreach(arr => println(arr.mkString(",")))
  }
}
