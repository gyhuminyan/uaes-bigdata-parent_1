package com.uaes.spark.analysis.KPIhandle

import com.alibaba.fastjson.{JSON, JSONObject}
import com.uaes.spark.analysis.utils.{DBUtils, Per100Utils, SparkUtil}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SQLContext
import org.slf4j.LoggerFactory

/**
  * Created by hand on 2017/10/25.
  */
object AirConFuel {
  val logger = LoggerFactory.getLogger(AirConFuel.getClass)

  def main(args: Array[String]): Unit = {
    val sc = SparkUtil.getSparkContext("WaitSpeedFuel",logger)
    val rdd = sc.textFile("H:/UAES/TestData.txt")
    everyDayAirConFuel(rdd)
    everyHunKiAirConFuel(rdd)
  }

  //每天空调耗油
  def everyDayAirConFuel(rDD: RDD[String]): Unit = {
    val jsonRDD = rDD.filter(line => {
      val jobj = JSON.parseObject(line)
      jobj.get("stype").equals("InsFuelInjection") ||
        jobj.get("stype").equals("airConditioningState")
    }).map(line => {
      val jobj = JSON.parseObject(line)
      val date = jobj.getString("timestamp").substring(8)
      val vin = jobj.getString("VIN")
      jobj.put("timestamp", date)
      (vin + "_" + date, jobj)
    }).groupByKey().map(pair => {
      val tmpObj = new JSONObject()
      val strs = pair._1.split("_")
      tmpObj.put("VIN", strs(0))
      tmpObj.put("timestamp", strs(1))
      tmpObj.put("airConditioningState", 0)
      tmpObj.put("InsFuelInjection", 0)
      for (jObj <- pair._2) {
        jObj.getString("stype") match {
          case "airConditioningState" => {
            val value = jObj.getString("value")
            tmpObj.put("airConditioningState", value)
          }
          case "InsFuelInjection" => {
            val value = jObj.getString("value").toInt
            tmpObj.put("InsFuelInjection", value)
          }
        }
      }
      tmpObj.toJSONString
    })

    val sqlContext = SQLContext.getOrCreate(rDD.sparkContext)
    val df = sqlContext.read.json(jsonRDD)
    df.createTempView("car")

    val resDF = sqlContext.sql("select VIN,subString(timestamp,0,8) as date,sum(InsFuelInjection) as totalFuel " +
      "from car where airConditioningState=0 group by VIN,subString(timestamp,0,8)")
    //保存到数据库
    val resRDD = resDF.rdd.map(row => {
      val vin = row.getString(0)
      val date = row.getString(1)
      val KPI = row.getString(2)
      val totalFuel = row.getString(3).toDouble
      (date, vin, KPI, totalFuel)
    })
    DBUtils.saveResultToDB(resRDD)
  }

  //每百公里空调耗油
  def everyHunKiAirConFuel(rDD: RDD[String]): Unit = {
    val per100UtilsRdd = Per100Utils.getAfterLastPer100Data(rDD, "3")
    val jsonRDD = per100UtilsRdd.filter(line => {
      val jobj = JSON.parseObject(line)
      jobj.get("stype").equals("airConditioningState") || //空调状态
        jobj.get("stype").equals("InsFuelInjection") || //瞬时喷油量
        jobj.get("stype").equals("drivingMileage") //行驶里程
    }).map(line => {
      val jobj = JSON.parseObject(line)
      val vin = jobj.getString("VIN")
      val drivingMileage = (jobj.getString("drivingMileage").toDouble / 100).toInt * 100
      val date = jobj.getString("timestamp").substring(14)
      jobj.put("timestamp", date)
      (vin + "_" + drivingMileage, jobj)
    }).groupByKey().map(pair => {
      val tmpObj = new JSONObject()
      val strs = pair._1.split("_")
      tmpObj.put("VIN", strs(0))
      tmpObj.put("everyHunKil", strs(1))
      tmpObj.put("airConditioningState", "0")
      tmpObj.put("InsFuelInjection", 0)
      for (jObj <- pair._2) {
        jObj.getString("stype") match {
          case "InsFuelInjection" => {
            val value = jObj.getString("value").toInt
            tmpObj.put("InsFuelInjection", value)
          }
          case "airConditioningState" => {
            val value = jObj.getString("value")
            tmpObj.put("airConditioningState", value)
          }
        }
      }
      tmpObj.toJSONString
    })

    val sqlContext = SQLContext.getOrCreate(rDD.sparkContext)
    val df = sqlContext.read.json(jsonRDD)
    df.createTempView("car")

    val resDF = sqlContext.sql("select VIN,everyHunKil,KPI,sum(InsFuelInjection) as totalFuel " +
      "from car where airConditioningState=0 group by VIN,everyHunKil")
    //保存到数据库
    val resRDD = resDF.rdd.map(row => {
      val vin = row.getString(0)
      val everyHunKil = row.getString(1)
      val KPI = row.getString(2)
      val totalFuel = row.getString(3).toDouble
      (everyHunKil, vin, KPI, totalFuel)
    })
    DBUtils.saveResultToDB(resRDD)
  }
}
