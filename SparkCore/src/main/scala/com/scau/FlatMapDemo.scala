package com.scau

import org.apache.spark.{SparkConf, SparkContext}

/**
 * description: 创建一个元素为 1-5 的RDD，运用 flatMap创建一个新的 RDD，
 * 新的 RDD 为原 RDD 每个元素的 平方和三次方 来组成 1,1,4,8,9,27..
 * 作用: 类似于map，但是每一个输入元素可以被映射为 0 或多个输出元素（所以func应该返回一个序列，而不是单一元素 T => TraversableOnce[U]
 * author: mhy 
 * date: 2021/3/15 
 */
object FlatMapDemo {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local[2]").setAppName(this.getClass.getName)
    val sc = new SparkContext(conf)
    val rdd1 = sc.parallelize(Array(1, 2, 3, 4, 5), 3)
    val rdd2 = rdd1.flatMap(x => List(x * x, x * x * x))
    rdd2.collect().foreach(println)
    sc.stop()
  }
}
