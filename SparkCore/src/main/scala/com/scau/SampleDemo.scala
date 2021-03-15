package com.scau

import org.apache.spark.{SparkConf, SparkContext}

/**
 * description: sample(withReplacement, fraction, seed)作用:
 * 1.	以指定的随机种子随机抽样出比例为fraction的数据，(抽取到的数量是: size * fraction). 需要注意的是得到的结果并不能保证准确的比例.
 * 2.	withReplacement表示是抽出的数据是否放回，true为有放回的抽样，false为无放回的抽样. 放回表示数据有可能会被重复抽取到, false 则不可能重复抽取到. 如果是false, 则fraction必须是:[0,1], 是 true 则大于等于0就可以了.
 * 3.	seed用于指定随机数生成器种子。 一般用默认的, 或者传入当前的时间戳
 * author: mhy 
 * date: 2021/3/15 
 */
object SampleDemo {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName(this.getClass.getName).setMaster("local[2]")
    val sc = new SparkContext(conf)
    val rdd1 = sc.parallelize(1 to 10)
    val rdd2 = rdd1.sample(true, 2)
    rdd2.collect.foreach(println)
    sc.stop()
  }
}
