package com.udbac.sparklog

import java.io.File
import java.net.URLDecoder

import com.udbac.analyser.constant.IPv4Handler
import com.udbac.constant.{IPv4Handler, LogConstants}
import com.udbac.logana.constant.LogConstants
import com.udbac.logana.utils.{IPv4Handler, TimeUtil}
import eu.bitwalker.useragentutils.UserAgent
import org.apache.commons.io.FileUtils
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable
import scala.util.parsing.json.JSON

/**
  * Created by root on 2017/1/10.
  */
object LogAnalyser {

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("LogAnalyser")
    val sc = new SparkContext(conf)
    if (args.length != 2) {
      sys.error("Usages: <input path> <output path>")
      sys.exit(-1)
    }
    val lineRDD = sc.textFile(args(0))
    val mapRDD = lineRDD.map(x => logParser(x)).saveAsTextFile(args(1))
    val fieldsStr = FileUtils.readFileToString(new File("fields.json"))
    JSON.parseFull(fieldsStr)

  }


  def logParser(lineStr: String): mutable.Map[String, String] = {
    val logMap = mutable.Map[String, String]()
    val lineSplits = lineStr.split(" ")
    if (lineSplits.length == 15) {
      val date_time= TimeUtil.handleTime(lineSplits(0)+" "+lineSplits(1))
      logMap.put(LogConstants.LOG_COLUMN_DATETIME, date_time)
      logMap.put(LogConstants.LOG_COLUMN_IP, lineSplits(2))
      logMap.put(LogConstants.LOG_COLUMN_IPCODE,IPv4Handler.getIPcode(lineSplits(2)))
      logMap.put(LogConstants.REGION_PROVINCE,IPv4Handler.getArea(lineSplits(2))(0))
      logMap.put(LogConstants.REGION_CITY,IPv4Handler.getArea(lineSplits(2))(1))
      logMap.put(LogConstants.LOG_COLUMN_USERNAME, lineSplits(3))
      logMap.put(LogConstants.LOG_COLUMN_HOST, lineSplits(4))
      logMap.put(LogConstants.LOG_COLUMN_METHOD, lineSplits(5))
      logMap.put(LogConstants.LOG_COLUMN_URISTEM, lineSplits(6))
      logMap.put(LogConstants.LOG_COLUMN_STATUS, lineSplits(8))
      logMap.put(LogConstants.LOG_COLUMN_BYTES, lineSplits(9))
      logMap.put(LogConstants.LOG_COLUMN_VERSION, lineSplits(10))
      logMap.put(LogConstants.LOG_COLUMN_COOKIE, lineSplits(12))
      logMap.put(LogConstants.LOG_COLUMN_REFERER, lineSplits(13))
      logMap.put(LogConstants.LOG_COLUMN_DCSID, lineSplits(14))
      handleQuery(logMap,lineSplits(7))
      handleUA(logMap, lineSplits(11))
    }
    logMap
  }

  def handleQuery(logMap: mutable.Map[String, String], queryStr: String): Unit = {
    if(!queryStr.isEmpty) {
      val querySplits = queryStr.split(LogConstants.QUERY_SEPARTIOR)
      for (querySplit <- querySplits) {
        val items = querySplit.split(LogConstants.QUERY_ITEM_SEPARTIOR)
        if (items.length == 2) {
          try {
            items(1) = URLDecoder.decode(items(1), "UTF-8")
          } catch {
            case ex: Exception => ex.printStackTrace()
        }
        logMap.put(items(0), items(1))
        }
      }
    }
  }

  def handleUA(logMap: mutable.Map[String, String], uaStr: String): Unit = {
    if (!uaStr.isEmpty) {
      val userAgent = UserAgent.parseUserAgentString(uaStr)
      logMap.put(LogConstants.UA_OS_NAME, userAgent.getOperatingSystem.getName)
      logMap.put(LogConstants.UA_BROWSER_NAME, userAgent.getBrowser.getName)
    }
  }
}
