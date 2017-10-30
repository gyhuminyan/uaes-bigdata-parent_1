package com.uaes.spark.analysis.KPIhandle

import com.alibaba.fastjson.{JSON, JSONObject}
import com.uaes.spark.analysis.utils.{DBUtils, Per100Utils, SparkUtil}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SQLContext
import org.slf4j.LoggerFactory

/**
  * Created by hand on 2017/10/24.
  */
object WaitSpeedFuel {
  lazy val logger = LoggerFactory.getLogger(WaitSpeedFuel.getClass)

  def main(args: Array[String]): Unit = {

    val sc = SparkUtil.getSparkContext("WaitSpeedFuel",logger)
    val rdd = sc.textFile("H:/UAES/TestData.txt")

    everyDayWaitFuel(rdd)
    everyHunKilWaitFuel(rdd)
  }

  //每天怠速耗油
  def everyDayWaitFuel(rDD: RDD[String]): Unit = {
    val jsonRDD = rDD.filter(line => {
      val jobj = JSON.parseObject(line)
      jobj.get("stype").equals("drivingSpeed") ||
        jobj.get("stype").equals("rotationlSpeed") ||
        jobj.get("stype").equals("InsFuelInjection")
    }).map(line => {
      val jobj = JSON.parseObject(line)
      val date = jobj.getString("timestamp").substring(15)
      val vin = jobj.getString("VIN")
      jobj.put("timestamp", date)
      (vin + "_" + date, jobj)
    }).groupByKey().map(pair => {
      val tmpObj = new JSONObject()
      val strs = pair._1.split("_")
      tmpObj.put("VIN", strs(0))
      tmpObj.put("timestamp", strs(1))
      tmpObj.put("rotationlSpeed", 0)
      tmpObj.put("drivingSpeed", 0)
      tmpObj.put("InsFuelInjection", 0)
      var rotationlSpeed = 0
      for (jObj <- pair._2) {
        jObj.getString("stype") match {
          case "rotationlSpeed" => {
            rotationlSpeed += jObj.getString("value").toInt
            tmpObj.put("rotationlSpeed", rotationlSpeed)
          }
          case "drivingSpeed" => {
            val value = jObj.getString("value").toInt
            tmpObj.put("drivingSpeed", value)
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

    val resDF = sqlContext.sql("select VIN,subString(timestamp 0,8) as date,KPI,sum(InsFuelInjection) as totalFuel " +
      "from car where drivingSpeed=0 and rotationlSpeed > 0 group by VIN,subString(timestamp 0,8)")
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

  //每百公里怠速耗油
  def everyHunKilWaitFuel(rDD: RDD[String]): Unit = {
    val per100UtilsRdd = Per100Utils.getAfterLastPer100Data(rDD, "1")
    val jsonRDD = per100UtilsRdd.filter(line => {
      val jobj = JSON.parseObject(line)
      jobj.get("stype").equals("drivingSpeed") || //行驶速度
        jobj.get("stype").equals("rotationlSpeed") || //转速
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
      tmpObj.put("rotationlSpeed", 0)
      tmpObj.put("drivingSpeed", 0)
      tmpObj.put("InsFuelInjection", 0)
      var rotationlSpeed = 0
      for (jObj <- pair._2) {
        jObj.getString("stype") match {
          case "rotationlSpeed" => {
            rotationlSpeed += jObj.getString("value").toInt
            tmpObj.put("rotationlSpeed", rotationlSpeed)
          }
          case "drivingSpeed" => {
            val value = jObj.getString("value").toInt
            tmpObj.put("drivingSpeed", value)
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

    val resDF = sqlContext.sql("select VIN,everyHunKil,KPI,sum(InsFuelInjection) as totalFuel " +
      "from car where drivingSpeed=0 and rotationlSpeed > 0 group by VIN,everyHunKil")
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
