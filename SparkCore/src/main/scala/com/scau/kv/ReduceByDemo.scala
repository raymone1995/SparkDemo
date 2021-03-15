package com.scau.kv

import org.apache.spark.{HashPartitioner, SparkConf, SparkContext}

/**
 * description: 在一个(K,V)的 RDD 上调用，返回一个(K,V)的 RDD，使用指定的reduce函数，
 * 将相同key的value聚合到一起，reduce任务的个数可以通过第二个可选的参数来设置
 * author: mhy 
 * date: 2021/3/15 
 */
object ReduceByDemo {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local[*]").setAppName("cartesian")
    val sc = new SparkContext(conf)
    val rdd1 = sc.parallelize(Array((1, "mhy"),(1, ":"),  (2, "lwx"), (3, "uzi"), (1, "long")))
    val rdd2 = rdd1.reduceByKey(_ + _)
    rdd2.collect.foreach(println)
    sc.stop()
  }
}
