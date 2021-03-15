package com.scau

import org.apache.spark.{SparkConf, SparkContext}

/**
 * description: 作用：求并集. 对源 RDD 和参数 RDD 求并集后返回一个新的 RDD
 * author: mhy 
 * date: 2021/3/15 
 */
object UnionDemo {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local[*]").setAppName("union")
    val sc = new SparkContext(conf)
    val rdd1 = sc.parallelize(1 to 10)
    val rdd2 = sc.parallelize(-10 to 0)
    val rdd3 = rdd1.union(rdd2).sortBy(x => x)
    rdd3.collect().foreach(println)
    sc.stop()
  }
}
