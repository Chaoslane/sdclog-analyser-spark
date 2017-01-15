package com.udbac.logana.utils;

import com.udbac.logana.constant.LogConstants;
import org.apache.commons.io.FileUtils;

import java.io.File;
import java.io.IOException;
import java.util.*;

/**
 * Created by root on 2017/1/12.
 * IP解析为地域名称
 */
public class IPv4Handler {
    private static final String fileSeges = "udbacIPtransSegs.csv";
    private static final String fileAreas = "udbacIPtransArea.csv";
    /**
     * 解析为 province,city
     * @param logIP IP字符串
     * @return  province,city
     * @throws IOException
     */
    public static String[] getArea(String logIP) throws IOException {
        List<String> readAreas = FileUtils.readLines(new File(fileAreas));
        Map<String, String[]> mapArea = new HashMap<>();
        for (String oneline : readAreas) {
            String[] strings = oneline.split(LogConstants.IPCSV_SEPARTIOR);
            mapArea.put(strings[2], strings);
        }
        return mapArea.get(getIPcode(logIP));
    }

    /**
     * 获取文件IPcode
     * @param logIP IP字符串
     * @return IPcode
     * @throws IOException
     */
    public static String getIPcode(String logIP) throws IOException {
        List<String> readSeges = FileUtils.readLines(new File(fileSeges));
        Map<Integer, String> ipMap = new HashMap<>();
        List<Integer> rangeList = new ArrayList<>();
        for (String oneline : readSeges) {
            String[] strings = oneline.split(LogConstants.IPCSV_SEPARTIOR);
            Integer startIP = IPv4Util.ipToInt(strings[0]);
            String code = strings[2];
            rangeList.add(startIP);
            ipMap.put(startIP, code);
        }
        //lambda表达式
        rangeList.sort(Integer::compareTo);
        //获取ip在有序list中位置
        Integer index = searchIP(rangeList, IPv4Util.ipToInt(logIP));
        return ipMap.get(rangeList.get(index));
    }
    /**
     * 二分查找 ip 在有序 list 中的 index
     * @param rangeList ip转化成整数后 sort
     * @param ipInt ipToInt
     * @return index
     */
    private static Integer searchIP(List<Integer> rangeList, Integer ipInt) {
        int mid = rangeList.size() / 2;
        if (rangeList.get(mid) == ipInt) {
            return mid;
        }
        int start = 0;
        int end = rangeList.size() - 1;
        while (start <= end) {
            mid = (end - start) / 2 + start;
            if (ipInt < rangeList.get(mid)) {
                end = mid - 1;
                if(ipInt > rangeList.get(mid-1)){
                    return mid - 1;
                }
            } else if (ipInt > rangeList.get(mid)) {
                start = mid + 1;
                if (ipInt < rangeList.get(mid + 1)) {
                    return mid;
                }
            } else {
                return mid;
            }
        }
        return 0;
    }
}
