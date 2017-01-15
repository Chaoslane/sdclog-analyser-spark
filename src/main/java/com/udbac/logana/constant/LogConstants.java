package com.udbac.logana.constant;

/**
 * Created by root on 2016/7/12.
 */
public class LogConstants {
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
    public static final String QUERY_ITEM_SEPARTIOR = "=";
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
     * 动作发生的日期时间
     */
    public static final String LOG_COLUMN_DATETIME = "datetime";
    /**
     * App客户端IP地址
     */
    public static final String LOG_COLUMN_IP = "c_ip";

    public static final String LOG_COLUMN_IPCODE = "ipcode";

    public static final String LOG_COLUMN_USERNAME = "cs_username";
    /**
     * App服务器地址，原生App数据采集数据中此项为空。
     */
    public static final String LOG_COLUMN_HOST = "cs_host";
    /**
     *请求中使用的http方法，get/post
     */
    public static final String LOG_COLUMN_METHOD = "cs_method";
    /**
     * 访问文件URI主体，原生App采集动作假名，HTML页面采集URI
     */
    public static final String LOG_COLUMN_URISTEM = "cs_uri_stem";
    /**
     * 记录URL查询参数，此字段中存放SDK采集的自定义业务与用户行为数据
     */
    public static final String LOG_COLUMN_URIQUERY = "cs_uri_query";

    /**
     * 记录http状态代码，200表示成功
     */
    public static final String LOG_COLUMN_STATUS = "cs_status";

    public static final String LOG_COLUMN_BYTES = "cs_bytes";

    public static final String LOG_COLUMN_VERSION = "cs_version";
    /**
     * 客户端浏览器、操作系统等情况，原生App采集中此数据为空。
     */
    public static final String LOG_COLUMN_USERAGENT = "cs_user_agent";
    /**
     * 客户端cookies内容，原生App采集中此数据为空。
     */
    public static final String LOG_COLUMN_COOKIE = "cs_cookie";
    /**
     * 用户此动作时的前向链接，原生App采集中此数据为空。
     */
    public static final String LOG_COLUMN_REFERER = "cs_referer";

    public static final String LOG_COLUMN_DCSID = "dcs_id";
    /**
     * 操作系统名称
     */
    public static final String UA_OS_NAME = "os";
    /**
     * 浏览器名称
     */
    public static final String UA_BROWSER_NAME = "browser";
    /**
     * 浏览器版本
     */
    public static final String REGION_PROVINCE = "province";
    public static final String REGION_CITY = "city";

    }
