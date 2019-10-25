package com.fhb.bigdata.spark

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * Created by IntelliJ IDEA 2019.2.3.
 * User: fhb
 * Email: fhb7218@gmail.com
 * Date: 2019/10/25
 * Time: 16:05
 *
 * @author hbfang
 */

object Spark14_aggregateByKey {

  def main(args: Array[String]): Unit = {
    val config: SparkConf = new SparkConf().setMaster("local").setAppName("Op4")

    val sc = new SparkContext(config)

    val listRDD: RDD[(String, Int)] = sc.makeRDD(List(("a", 2), ("a", 2), ("c", 4), ("b", 3), ("c", 6), ("c", 8)), 2)

    listRDD.glom().collect().foreach(arr => println(arr.mkString(",")))

    val aggregateByKeyRDD: RDD[(String, Int)] = listRDD.aggregateByKey(0)(math.max(_, _), _ + _)

    aggregateByKeyRDD.collect().foreach(println)
  }

}
