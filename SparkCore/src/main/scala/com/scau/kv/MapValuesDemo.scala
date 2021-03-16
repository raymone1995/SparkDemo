package com.scau.kv

import org.apache.spark.{SparkConf, SparkContext}

/**
 * description: 作用: 针对(K,V)形式的类型只对V进行操作
 * author: mhy 
 * date: 2021/3/16 
 */
object MapValuesDemo {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local[*]").setAppName("sortbykey")
    val sc = new SparkContext(conf)
    val rdd1 = sc.parallelize(Array(("a", 1), ("c", 5), ("b", 16), ("b", 12)), 4)
    rdd1.mapValues(_ + 1).collect.foreach(println)
    sc.stop()
  }
}
