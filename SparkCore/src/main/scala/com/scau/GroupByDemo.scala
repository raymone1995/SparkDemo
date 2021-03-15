package com.scau

import org.apache.spark.{SparkConf, SparkContext}

/**
 * description: func返回值作为 key, 对应的值放入一个迭代器中. 返回的 RDD: RDD[(K, Iterable[T])
 * 每组内元素的顺序不能保证, 并且甚至每次调用得到的顺序也有可能不同.
 * 创建一个 RDD，按照元素的奇偶性进行分组
 * author: mhy 
 * date: 2021/3/15 
 */
object GroupByDemo {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local[*]").setAppName(this.getClass.getName)
    val sc = new SparkContext(conf)
    val rdd1 = sc.parallelize(Array(1, 2, 3, 4, 5, 6, 7, 8, 9, 10))
    val rdd2 = rdd1.groupBy(x => if(x % 2 == 0) "even" else "odd")
    rdd2.collect().foreach(x => {
      print(x._1 + ": ")
      for(item <- x._2) {
        print(item + "\t")
      }
      println
    })
  }
}
