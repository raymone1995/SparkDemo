package com.scau

import org.apache.spark.{SparkConf, SparkContext}

/**
 * description: 和mapPartitions(func)类似. 但是会给func多提供一个Int值来表示分区的索引
 * author: mhy 
 * date: 2021/3/15 
 */
object MapPartitionsWithIndexDemo {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local[*]").setAppName(this.getClass.getName)
    val sc = new SparkContext(conf)
    val rdd1 = sc.parallelize(Array(1, 2, 3, 4, 5, 6, 7, 8, 9, 10), 2)
    val rdd2 = rdd1.mapPartitionsWithIndex((index, it) => {
      it.map((index, _))
    })
    rdd2.collect().foreach(println)
    sc.stop()
  }
}
