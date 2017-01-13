package com.udbac.constant;

/**
 * Created by root on 2016/7/12.
 */
public class SDCLogConstants {
    /**
     * 默认分隔符
     */
    public static final String LOG_SEPARTIOR = " ";
    /**
     * 默认分隔符
     */
    public static final String QUERY_SEPARTIOR = "&";
    /**
     * 默认分隔符
     */
    public static final String IPCSV_SEPARTIOR = "\t";
    /**
     * 默认值
     */
    public static final String DEFAULT_VALUE = "unknown";
    /**
     * 一天的毫秒数
     */
    public static final int DAY_OF_MILLISECONDS = 86400000;
    /**
     * 半小时的毫秒数
     */
    public static final int HALFHOUR_OF_MILLISECONDS = 1800000;
    /**
     * 定义的运行时间变量名
     */
    public static final String RUNNING_DATE_PARAMES = "RUNNING_DATE";
    /**
     * 动作发生的日期时间
     */
    public static final String LOG_COLUMN_NAME_DATE_TIME = "date_time";
    /**
     * App客户端IP地址
     */
    public static final String LOG_COLUMN_NAME_CIP = "cip";
    public static final String LOG_COLUMN_NAME_CS_USERNAME = "cs-username";
    /**
     * App服务器地址，原生App数据采集数据中此项为空。
     */
    public static final String LOG_COLUMN_CS_HOST = "cs_host";
    /**
     *请求中使用的http方法，get/post
     */
    public static final String LOG_COLUMN_NAME_CSMETHOD = "cs_method";
    /**
     * 访问文件URI主体，原生App采集动作假名，HTML页面采集URI
     */
    public static final String LOG_COLUMN_NAME_CSURISTEM = "cs_uri_stem";
    /**
     * 记录URL查询参数，此字段中存放SDK采集的自定义业务与用户行为数据
     */
    public static final String LOG_COLUMN_NAME_CSURIQUERY = "cs_uri_query";
    /**
     * 记录http状态代码，200表示成功
     */
    public static final String LOG_COLUMN_NAME_SCSTATUS = "sc_status";
    /**
     * HTTP协议版本，如http/1.0
     */
    public static final String LOG_COLUMN_NAME_CSVERSION = "cs_version";
    /**
     * 客户端浏览器、操作系统等情况，原生App采集中此数据为空。
     */
    public static final String LOG_COLUMN_NAME_CSUSERAGENT = "cs_user_agent";
    /**
     * 客户端cookies内容，原生App采集中此数据为空。
     */
    public static final String LOG_COLUMN_NAME_CSCOOKIE = "cs_cookie";
    /**
     * 用户此动作时的前向链接，原生App采集中此数据为空。
     */
    public static final String LOG_COLUMN_NAME_CSREFERER = "cs_referer";
    /**
     * 操作系统名称
     */
    public static final String LOG_COLUMN_NAME_OS_NAME = "os";
    /**
     * 操作系统版本
     */
    public static final String LOG_COLUMN_NAME_OS_VERSION = "os_v";
    /**
     * 浏览器名称
     */
    public static final String LOG_COLUMN_NAME_BROWSER_NAME = "browser";
    /**
     * 浏览器版本
     */
    public static final String LOG_COLUMN_NAME_COUNTRY = "country";
    public static final String LOG_COLUMN_NAME_PROVINCE = "province";
    public static final String LOG_COLUMN_NAME_CITY = "city";
    public static final String LOG_COLUMN_NAME_DCSID = "dcsid";

    }
