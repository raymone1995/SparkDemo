package com.scau

import org.apache.spark.{SparkConf, SparkContext}

/**
 * description: 管道，针对每个分区，把 RDD 中的每个数据通过管道传递给shell命令或脚本，返回输出的RDD。
 * 一个分区执行一次这个命令. 如果只有一个分区, 则执行一次命令
 * author: mhy 
 * date: 2021/3/15 
 */
object PipeDemo {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local[2]").setAppName("pipe")
    val sc = new SparkContext(conf)
    val rdd1 = sc.parallelize(1 to 10, 2)
    val rdd2 = rdd1.pipe("echo hihi ")
    rdd2.collect.foreach(println)
    sc.stop()
  }
}
