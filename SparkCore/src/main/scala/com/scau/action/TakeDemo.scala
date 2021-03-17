package com.scau.action

import org.apache.spark.{SparkConf, SparkContext}

/**
 * 返回 RDD 中前 n 个元素组成的数组.
 * take 的数据也会拉到 driver 端, 应该只对小数据集使用
 */
object TakeDemo {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local[2]").setAppName("take")
    val sc = new SparkContext(conf)
    val rdd1 = sc.parallelize(Array(("mhy", 24), ("logn", 43), ("beich", 48), ("doinb", 19)))
    val rdd2 = rdd1.sortBy(_._2, true).take(3)
    rdd2.foreach(println)
    sc.stop()
  }
}
