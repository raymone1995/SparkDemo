package com.scau.kv

import org.apache.spark.{SparkConf, SparkContext}

/**
 * description: 使用给定的 combine 函数和一个初始化的zero value, 对每个key的value进行聚合.
 * 这个函数返回的类型U不同于源 RDD 中的V类型. U的类型是由初始化的zero value来定的. 所以, 我们需要两个操作:
 * - 一个操作(seqOp)去把 1 个v变成 1 个U - 另外一个操作(combOp)来合并 2 个U
 * 第一个操作用于在一个分区进行合并, 第二个操作用在两个分区间进行合并.
 * 为了避免内存分配, 这两个操作函数都允许返回第一个参数, 而不用创建一个新的U
 * 参数描述:
 * 1.	zeroValue：给每一个分区中的每一个key一个初始值；
 * 2.	seqOp：函数用于在每一个分区中用初始值逐步迭代value；
 * 3.	combOp：函数用于合并每个分区中的结果。
 * author: mhy 
 * date: 2021/3/15 
 */
object AggregateByKeyDemo {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local[*]").setAppName("aggregateByKey")
    val sc = new SparkContext(conf)
    val rdd1 = sc.parallelize(Array(("a", 1), ("b", 3), ("a", 4), ("b", 2), ("b", 8)), 2)
    val rdd2 = rdd1.aggregateByKey(Int.MinValue)(math.max(_, _), _ + _)
    rdd2.collect.foreach(println)
    sc.stop()
  }
}
