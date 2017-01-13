package com.udbac.sparklog

import java.net.URLDecoder
import com.udbac.constant.SDCLogConstants
import java.io.UnsupportedEncodingException
import eu.bitwalker.useragentutils.UserAgent
import org.apache.spark.{SparkConf, SparkContext}
import scala.collection.mutable

/**
  * Created by root on 2017/1/10.
  */
object LogAnalyser {

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("LogAnalyser")
    val sc = new SparkContext(conf)
    val lineRDD = sc.textFile(args(0))
    val mapRDD = lineRDD.map(x => logParser(x)).saveAsTextFile(args(1))
  }


  def logParser(lineStr: String): mutable.Map[String, String] = {
    val logMap = mutable.Map[String, String]()
    val lineSplits = lineStr.split(" ")
    if(lineSplits.length==15){
      logMap.put(SDCLogConstants.LOG_COLUMN_NAME_DATE_TIME, lineSplits(0) + lineSplits(1))
      logMap.put(SDCLogConstants.LOG_COLUMN_CS_HOST, lineSplits(4))
      logMap.put(SDCLogConstants.LOG_COLUMN_NAME_CSMETHOD, lineSplits(5))
      logMap.put(SDCLogConstants.LOG_COLUMN_NAME_CSURISTEM, lineSplits(6))
      logMap.put(SDCLogConstants.LOG_COLUMN_NAME_SCSTATUS, lineSplits(8))
      logMap.put(SDCLogConstants.LOG_COLUMN_NAME_DCSID, lineSplits(14))
      handleQuery(logMap, lineSplits(7))
      handleUA(logMap, lineSplits(11))
    }
    logMap
  }

  def handleQuery(logMap: mutable.Map[String, String], queryStr: String): Unit = {
    if (queryStr.length > 10) {
      val querySplits = queryStr.split("&")
      for (querySplit <- querySplits) {
        val items = querySplit.split("=")
        if (items.length == 2) {
          try {
            items(1) = URLDecoder.decode(items(1), "UTF-8")
          } catch {
            case ex : Exception => ex.printStackTrace()
          }
          logMap.put(items(0),items(1))
        }
      }
    }
  }

  def handleUA(logMap: mutable.Map[String, String], uaStr: String): Unit = {
    if (!uaStr.equals(null)) {
      val userAgent = UserAgent.parseUserAgentString(uaStr)
      logMap.put(SDCLogConstants.LOG_COLUMN_NAME_OS_NAME,userAgent.getOperatingSystem.getName)
      logMap.put(SDCLogConstants.LOG_COLUMN_NAME_BROWSER_NAME,userAgent.getBrowser.getName)
    }
  }
}
