package com.scau

import org.apache.spark.{SparkConf, SparkContext}

/**
 * description: 创建一个包含1-10的的 RDD，然后将每个元素*2形成新的 RDD
 * author: mhy 
 * date: 2021/3/15 
 */
object MapDemo {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local[*]").setAppName("mapdemo")
    val sc = new SparkContext(conf)
    val rdd1 = sc.parallelize(Array(1, 2, 3, 4, 5, 6, 7, 8, 9, 10))
    val rdd2 = rdd1.map(_ * 2)
    rdd2.collect().foreach(println)
    sc.stop()
  }

}
