package com.scau.kv

import org.apache.spark.{SparkConf, SparkContext}

/**
 * description: aggregateByKey的简化操作，seqop和combop相同
 * author: mhy 
 * date: 2021/3/16 
 */
object FoldByKey {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local[*]").setAppName("FoldByKey")
    val sc = new SparkContext(conf)
    val rdd1 = sc.parallelize(Array(("a", 2), ("a", 5), ("b", 1), ("a", -1), ("b", 71)), 2)
    val rdd2 = rdd1.foldByKey(0)(_ + _)
    rdd2.collect.foreach(println)
    sc.stop()
  }
}
