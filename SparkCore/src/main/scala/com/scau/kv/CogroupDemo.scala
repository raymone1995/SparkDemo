package com.scau.kv

import org.apache.spark.{SparkConf, SparkContext}

/**
 * description: 作用：在类型为(K,V)和(K,W)的 RDD 上调用，
 * 返回一个(K,(Iterable<V>,Iterable<W>))类型的 RDD
 * author: mhy 
 * date: 2021/3/16 
 */
object CogroupDemo {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local[*]").setAppName("sortbykey")
    val sc = new SparkContext(conf)
    val rdd1 = sc.parallelize(Array(("mhy", 24), ("lim", 25), ("xihua", 28), ("army", 11), ("xlb", 20)))
    val rdd2 = sc.parallelize(Array(("mhy", "male"), ("mhy", 25), ("lim", "male"), ("xihua", "female"), ("army", "female")))
    rdd1.cogroup(rdd2).collect.foreach(println)
    sc.stop()
  }
}
