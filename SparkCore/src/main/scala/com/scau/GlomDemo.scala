package com.scau

import org.apache.spark.{SparkConf, SparkContext}

/**
 * description: 作用: 将每一个分区的元素合并成一个数组，形成新的 RDD 类型是RDD[Array[T]]
 * 创建一个 4 个分区的 RDD，并将每个分区的数据放到一个数组
 * author: mhy 
 * date: 2021/3/15 
 */
object GlomDemo {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local[*]").setAppName(this.getClass.getName)
    val sc = new SparkContext(conf)
    val rdd1 = sc.parallelize(Array(1, 2, 3, 4, 5, 6, 7, 8), 4)
    val rdd2 = rdd1.glom()
    rdd2.collect().foreach(arr => {
      for (item <- arr) {
        print(item + " ")
      }
      println()
    })
    sc.stop()
  }
}
