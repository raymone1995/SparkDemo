package com.scau.action

import org.apache.spark.{SparkConf, SparkContext}

/**
 * 通过func函数聚集 RDD 中的所有元素，先聚合分区内数据，再聚合分区间数据。
 */
object ReduceDemo {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local[*]").setAppName("reduce")
    val sc = new SparkContext(conf)
    val rdd1 = sc.parallelize(Array(("a", 1), ("b", 2), ("c", 3)))
    val rdd2 = rdd1.reduce((x, y) => (x._1 + y._1, x._2 + y._2))
    print(rdd2.toString())
    sc.stop()
  }
}
