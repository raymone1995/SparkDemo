package com.scau

import org.apache.spark.{SparkConf, SparkContext}

/**
 * description: 作用:
 * 对 RDD 中元素执行去重操作. 参数表示任务的数量.默认值和分区数保持一致.
 * author: mhy 
 * date: 2021/3/15 
 */
object DistinctDemo {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName(this.getClass.getName).setMaster("local[2]")
    val sc = new SparkContext(conf)
    val rdd1 = sc.parallelize(Array("mhy", "mhy", "hello", "hello world", "lwx", "hello"), 2)
    val rdd2 = rdd1.distinct
    rdd2.collect.foreach(println)
    sc.stop()
  }
}
