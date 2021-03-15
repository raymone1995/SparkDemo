package com.scau

import org.apache.spark.{SparkConf, SparkContext}

/**
 * description: 类似于map(func), 但是是独立在每个分区上运行.
 * 假设有N个元素，有M个分区，那么map的函数的将被调用N次,而mapPartitions被调用M次,一个函数一次处理所有分区
 * author: mhy 
 * date: 2021/3/15 
 */
object MapPartitions {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local[*]").setAppName("MapPartitions")
    val sc = new SparkContext(conf)
    val rdd1 = sc.parallelize(Array(1, 2, 3, 4, 5, 6, 7, 8, 9, 10), 2)
    val rdd2 = rdd1.mapPartitions(iterator => iterator.map(_ * 2))
    rdd2.collect().foreach(println)
    sc.stop()
  }
}
