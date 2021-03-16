package com.scau.kv

import org.apache.spark.{SparkConf, SparkContext}

/**
 * description: 在一个(K,V)的 RDD 上调用, K必须实现 Ordered[K] 接口
 * (或者有一个隐式值: Ordering[K]), 返回一个按照key进行排序的(K,V)的 RDD
 * author: mhy 
 * date: 2021/3/16 
 */
object SortByKeyDemo {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local[*]").setAppName("sortbykey")
    val sc = new SparkContext(conf)
    val rdd1 = sc.parallelize(Array(("a", 1), ("c", 5), ("b", 16), ("b", 12)), 4)
    val rdd2 = rdd1.sortByKey(false)
    rdd2.collect.foreach(println)
    sc.stop()
  }
}
