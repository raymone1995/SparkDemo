package com.scau

import org.apache.spark.{SparkConf, SparkContext}

/**
 * description: 作用: 过滤. 返回一个新的 RDD 是由func的返回值为true的那些元素组成
 * 创建一个 RDD（由字符串组成），过滤出一个新 RDD（包含“xiao”子串）
 * author: mhy 
 * date: 2021/3/15 
 */
object FilterDemo {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local[2]").setAppName(this.getClass.getName)
    val sc = new SparkContext(conf)
    val rdd1 = sc.parallelize(List("mohongyuan", "xiaomo", "Lwx", "xiaoxiang"), 2)
    val rdd2 = rdd1.filter(x => x.contains("xiao"))
    rdd2.collect.foreach(println)
    sc.stop()
  }
}
