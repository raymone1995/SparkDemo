package com.scau

import org.apache.spark.{SparkConf, SparkContext}

/**
 * description: 计算 2 个 RDD 的笛卡尔积. 尽量避免使用
 * author: mhy 
 * date: 2021/3/15 
 */
object CartesianDemo {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local[*]").setAppName("cartesian")
    val sc = new SparkContext(conf)
    val rdd1 = sc.parallelize(1 to 5)
    val rdd2 = sc.parallelize(Array("m", "o", "h"))
    val rdd3 = rdd1.cartesian(rdd2)
    rdd3.collect.foreach(println)
    sc.stop()
  }
}
