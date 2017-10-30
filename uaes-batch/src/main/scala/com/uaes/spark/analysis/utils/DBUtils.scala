package com.uaes.spark.analysis.utils

import org.apache.spark.rdd.RDD

import scala.collection.mutable.ArrayBuffer

/**
  * Created by Flink on 2017/10/24.
  */
object DBUtils {

  //将指标计算结果保存到MySql数据库
  def saveResultToDB(rdd: RDD[(String, String, String, Double)]): Unit ={
    JDBCWrapper.getInstance().truncate("car")
    rdd.foreachPartition(partition =>{
      val paramList = new ArrayBuffer[Array[Any]]()
      partition.foreach(tuple3 => {
        val carVin = tuple3._1
        val timestamp = tuple3._2
        val KPI = tuple3._3
        val avgVal = tuple3._4
         paramList += Array(carVin, timestamp, KPI, avgVal)
      })
      val sqlText = """insert into car_fuel(VIN, timestamp, kpi, avgVal) values(?, ?, ?, ?)"""
      val result = JDBCWrapper.getInstance().doBatch(sqlText, paramList.toArray)
    })
  }
}
