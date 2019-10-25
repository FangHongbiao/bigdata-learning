package com.fhb.bigdata.spark

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * Created by IntelliJ IDEA 2016.3.3.
 * User: fhb
 * Email: fhb7218@gmail.com
 * Date: 2019/10/25
 * Time: 9:08
 *
 * @author hbfang
 */

/**
 * groupBy 算子
 */
object Spark07_Oper6 {

  def main(args: Array[String]): Unit = {
    val config: SparkConf = new SparkConf().setMaster("local").setAppName("Op4")

    val sc = new SparkContext(config)

    val listRDD: RDD[Int] = sc.makeRDD(List(1,2,3,4))

    val groupByRDD: RDD[(Int, Iterable[Int])] = listRDD.groupBy(_%2)

    groupByRDD.collect().foreach(println)
  }
}
