package com.scau.kv

import org.apache.spark.{SparkConf, SparkContext}

/**
 * description: 按照key进行分组
 * author: mhy 
 * date: 2021/3/15 
 */
object GroupByKey {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local[2]").setAppName("groupByKey")
    val sc = new SparkContext(conf)
    val rdd1 = sc.parallelize(Array("ssll", "fofo", "dodo", "fofo", "doinb"))
    val rdd2 = rdd1.map((_, 1)).groupByKey()
    val rdd3 = rdd2.map(t => {
      (t._1, t._2.sum)
    })
    rdd3.collect.foreach(println)
    sc.stop()
  }
}
