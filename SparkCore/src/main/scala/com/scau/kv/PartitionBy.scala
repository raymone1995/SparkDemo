package com.scau.kv

import org.apache.spark.rdd.RDD
import org.apache.spark.{HashPartitioner, SparkConf, SparkContext}

/**
 * description: 对 pairRDD 进行分区操作，如果原有的 partionRDD 的分区器和传入的分区器相同,
 * 则返回原 pairRDD，否则会生成 ShuffleRDD，即会产生 shuffle 过程
 * author: mhy 
 * date: 2021/3/15 
 */
object PartitionBy {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local[*]").setAppName("cartesian")
    val sc = new SparkContext(conf)
    val rdd1 = sc.parallelize(Array((1, "mhy"), (2, "lwx"), (3, "uzi")))
    val rdd2 = rdd1.partitionBy(new HashPartitioner(3))
    rdd2.collect.foreach(println)
    sc.stop()
  }

}
