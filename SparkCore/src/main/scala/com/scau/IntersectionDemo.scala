package com.scau

import org.apache.spark.{SparkConf, SparkContext}

/**
 * description: 计算交集. 对源 RDD 和参数 RDD 求交集后返回一个新的 RDD
 * author: mhy 
 * date: 2021/3/15 
 */
object IntersectionDemo {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local[2]").setAppName("intersection")
    val sc = new SparkContext(conf)
    val rdd1 = sc.parallelize(1 to 10)
    val rdd2 = sc.parallelize(5 to 15)
    val rdd3 = rdd1.intersection(rdd2)
    rdd3.collect.foreach(println)
    sc.stop()
  }
}
