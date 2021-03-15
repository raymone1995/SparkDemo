package com.scau

import org.apache.spark.{SparkConf, SparkContext}

/**
 * description: 拉链操作. 需要注意的是, 在 Spark 中,
 * 两个 RDD 的元素的数量和分区数都必须相同,
 * 否则会抛出异常.(在 scala 中, 两个集合的长度可以不同)
 * author: mhy 
 * date: 2021/3/15 
 */
object ZipDemo {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local[*]").setAppName("cartesian")
    val sc = new SparkContext(conf)
    val rdd1 = sc.parallelize(1 to 3)
    val rdd2 = sc.parallelize(Array("m", "o", "h"))
    val rdd3 = rdd1.zip(rdd2)
    rdd3.collect.foreach(println)
    sc.stop()
  }
}
