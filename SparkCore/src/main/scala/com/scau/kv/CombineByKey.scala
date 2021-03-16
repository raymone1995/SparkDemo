package com.scau.kv

import org.apache.spark.{SparkConf, SparkContext}

/**
 * description: 创建一个 pairRDD，根据 key 计算每种 key 的value的平均值。
 * （先计算每个key出现的次数以及可以对应值的总和，再相除得到结果
 * author: mhy 
 * date: 2021/3/16 
 */
object CombineByKey {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local[*]").setAppName("combineByKey")
    val sc = new SparkContext(conf)
    val rdd1 = sc.parallelize(Array(("a", 1), ("a", 5), ("b", 6), ("b", 12)))
    val rdd2 = rdd1.combineByKey((_, 1), (acc: (Int, Int), v) => (acc._1 + v, acc._2 + 1),
      (acc1: (Int, Int), acc2: (Int, Int)) => (acc1._1 + acc2._1, acc1._2 + acc2._2))
    rdd2.map(t => (t._1, t._2._1.toDouble / t._2._2)).collect.foreach(println)
    sc.stop()
  }
}
