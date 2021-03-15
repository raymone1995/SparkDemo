package com.scau

import org.apache.spark.{SparkConf, SparkContext}

/**
 * description: 作用: 使用func先对数据进行处理，按照处理后结果排序，默认为正序。
 * author: mhy 
 * date: 2021/3/15 
 */
object SortByDemo {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local[*]").setAppName("sortBy")
    val sc = new SparkContext(conf)
    val rdd1 = sc.parallelize(Array("xiongbai", "hello", "xiang", "xixi"))
    val rdd2 = rdd1.sortBy(x => x, true)
    rdd2.collect().foreach(println)
    sc.stop()
  }

}
