package com.scau.example

import org.apache.spark.{SparkConf, SparkContext}

/**
 * description: 1.	数据结构：
 * 时间戳，       省份，城市，用户，广告，字段使用空格分割。
 * 1516609143867 6    7     64  16
 * 1516609143869 9    4     75  18
 * 1516609143869 1    7     87  12
 * 2.	需求: 统计出每一个省份广告被点击次数的 TOP3
 * author: mhy 
 * date: 2021/3/16 
 */
object Example01 {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local[*]").setAppName("example01")
    val sc = new SparkContext(conf)
    val lines = sc.textFile("D:\\SparkDemo\\SparkCore\\src\\main\\resources")
    val rdd1 = lines.map(line => {
      val arr = line.split("\\W")
      ((arr(1), arr(4)), 1)
    })
    val rdd2 = rdd1.reduceByKey(_ + _)
    val rdd3 = rdd2.map(t => (t._1._1, (t._1._2, t._2)))
    val rdd4 = rdd3.groupByKey()
    val rdd5 = rdd4.mapValues(t => {
      t.toList.sortBy(_._2)(Ordering.Int.reverse).take(3)
    })
    rdd5.collect.foreach(println)
    sc.stop()
  }
}
