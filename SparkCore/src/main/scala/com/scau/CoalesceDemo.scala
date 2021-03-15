package com.scau

import org.apache.spark.{SparkConf, SparkContext}

/**
 * description: 作用: 缩减分区数到指定的数量，用于大数据集过滤后，提高小数据集的执行效率。
 * author: mhy 
 * date: 2021/3/15 
 */
object CoalesceDemo {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local[*]").setAppName(this.getClass.getName)
    val sc = new SparkContext(conf)
    val rdd1 = sc.parallelize(Array(3, 7, 9, 2, 5, 100, -1), 10)
//    println(rdd1.partitions.length)
    val rdd2 = rdd1.coalesce(2)
//    println(rdd2.partitions.length)
    rdd2.collect.foreach(println)
    sc.stop()
  }
}
