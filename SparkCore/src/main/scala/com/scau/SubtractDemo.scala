package com.scau

import org.apache.spark.{SparkConf, SparkContext}

/**
 * description: 计算差集. 从原 RDD 中减去 原 RDD 和 otherDataset 中的共同的部分
 * author: mhy 
 * date: 2021/3/15 
 */
object SubtractDemo {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local[*]").setAppName("subtract")
    val sc = new SparkContext(conf)
    val rdd1 = sc.parallelize(1 to 10)
    val rdd2 = sc.parallelize(5 to 15)
    val rdd3 = rdd1.subtract(rdd2)
    rdd3.collect.foreach(println)
    sc.stop()
  }
}
