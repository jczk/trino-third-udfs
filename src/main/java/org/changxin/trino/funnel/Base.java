package org.changxin.trino.funnel;


import io.airlift.slice.Slice;
import io.airlift.slice.Slices;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * 漏斗的基础信息
 *
 */
public class Base {

    //查询的漏斗事件和索引关系：{events：{event：index，...},...}
    public static Map<Slice,Map<Slice,Byte>> event_pos_dict = new HashMap<>();

    public static void initEvents(Slice events){
        //漏斗事件之间逗号分割
        List<String> fs = Arrays.asList(new String(events.getBytes()).split(","));

        Map<Slice,Byte> pos_dict = new HashMap<>();
        for (byte i = 0; i < fs.size(); i++) {
            //每次添加值为<Slice,index>
            pos_dict.put(Slices.utf8Slice(fs.get(i)),i);
        }
        event_pos_dict.put(events,pos_dict);
    }
}
