package com.scau

import org.apache.spark.{SparkConf, SparkContext}

/**
 * description: 作用: 根据新的分区数, 重新 shuffle 所有的数据, 这个操作总会通过网络
 * 新的分区数相比以前可以多, 也可以少
 * author: mhy 
 * date: 2021/3/15 
 */
object RepartitionDemo {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local[*]").setAppName(this.getClass.getName)
    val sc = new SparkContext(conf)
    val rdd1 = sc.parallelize(1 to 10, 2)
    val rdd2 = rdd1.repartition(3)
    println(rdd2.partitions.length)
    rdd2.collect
    sc.stop()
  }
}
