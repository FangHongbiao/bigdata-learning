package com.fhb.bigdata.spark

import org.apache.spark.rdd.RDD
import org.apache.spark.{HashPartitioner, Partitioner, SparkConf, SparkContext}

/**
 * Created by IntelliJ IDEA 2019.2.3.
 * User: fhb
 * Email: fhb7218@gmail.com
 * Date: 2019/10/25
 * Time: 15:11
 *
 * @author hbfang
 */

/**
 * partitionBy算子
 */
object Spark13_Oper12_partitionBy {

  def main(args: Array[String]): Unit = {
    val config: SparkConf = new SparkConf().setMaster("local").setAppName("Op4")

    val sc = new SparkContext(config)

    // val partitionBy 要求(K, V)
    val listRDD: RDD[(Int, String)] = sc.makeRDD(Array((1, "aaa"), (2, "bbb"), (3, "ccc"), (4, "ddd")), 4)

//    val partitionByRDD: RDD[(Int, String)] = listRDD.partitionBy(new HashPartitioner(2))
    val partitionByRDD: RDD[(Int, String)] = listRDD.partitionBy(new MyPartition(2))

    partitionByRDD.glom().collect().foreach(arr => println(arr.mkString(",")))

  }

}

// 自定义分区器
class MyPartition (partitions : Int) extends Partitioner {
  override def numPartitions: Int = {
    partitions
  }
  override def getPartition(key: Any): Int = {
    1
  }
}
