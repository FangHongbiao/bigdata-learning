package com.fhb.bigdata.spark

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * Created by IntelliJ IDEA 2016.3.3.
 * User: fhb
 * Email: fhb7218@gmail.com
 * Date: 2019/10/25
 * Time: 9:11
 *
 * @author hbfang
 */

/**
 * filter 算子
 */
object Spark08_Oper7 {

  def main(args: Array[String]): Unit = {
    val config: SparkConf = new SparkConf().setMaster("local").setAppName("Op4")

    val sc = new SparkContext(config)

    val listRDD: RDD[Int] = sc.makeRDD(List(1,2,3,4))

    val filterRDD: RDD[Int] = listRDD.filter(_%2==0)

    filterRDD.collect().foreach(println)
  }
}
