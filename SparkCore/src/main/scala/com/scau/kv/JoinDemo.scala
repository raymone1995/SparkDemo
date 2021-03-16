package com.scau.kv

import org.apache.spark.{SparkConf, SparkContext}

/**
 * description: 内连接:
 * 在类型为(K,V)和(K,W)的 RDD 上调用，返回一个相同 key 对应的所有元素对在一起的(K,(V,W))的RDD
 * author: mhy 
 * date: 2021/3/16 
 */
object JoinDemo {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local[*]").setAppName("sortbykey")
    val sc = new SparkContext(conf)
    val rdd1 = sc.parallelize(Array(("mhy", 24), ("lim", 25), ("xihua", 28), ("army", 11), ("xlb", 20)))
    val rdd2 = sc.parallelize(Array(("mhy", "male"), ("lim", "male"), ("xihua", "female"), ("army", "female")))
    val rdd3 = rdd1.leftOuterJoin(rdd2)
    rdd3.collect.foreach(println)
    sc.stop()
  }
}
