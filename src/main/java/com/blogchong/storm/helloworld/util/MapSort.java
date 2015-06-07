package com.blogchong.storm.helloworld.util;

import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

/**
 * @Author: blogchong
 * @Blog: www.blogchong.com
 * @米特吧大数据论坛　www.mite8.com
 * @Mailbox: blogchong@163.com
 * @QQGroup: 191321336
 * @Weixin: blogchong
 * @Data: 2015/4/7
 * @Describe: Map排序工具
 */

public class MapSort {

    //对map进行排序，并且进行长度截取
    @SuppressWarnings({"unchecked", "rawtypes"})
    public static Map<String, Integer> sortByValue(Map<String, Integer> map) {

        if (map == null) {
            return null;
        }

        List list = new LinkedList(map.entrySet());

        Collections.sort(list, new Comparator() {

            public int compare(Object o1, Object o2) {
                Comparable sort1 = (Comparable) ((Map.Entry) o1).getValue();
                Comparable sort2 = (Comparable) ((Map.Entry) o2).getValue();
                return sort2.compareTo(sort1);
            }

        });

        Map result = new LinkedHashMap();

        for (Iterator it = list.iterator(); it.hasNext(); ) {

            Map.Entry entry = (Map.Entry) it.next();
            result.put(entry.getKey(), entry.getValue());

        }

        return result;
    }

    public static void main(String[] args) {

        Map<String, Integer> map = new HashMap<String, Integer>();
        map.put("test", 3);
        map.put("hcy", 1);
        map.put("put", 2);

        map = sortByValue(map);

        for (String key : map.keySet()) {
            System.out.println(key + " ==> " + map.get(key));
        }
    }

}
