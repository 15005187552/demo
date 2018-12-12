package com.kevin.mapreduce;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

import java.util.HashMap;

/**
 * 对数据进行分片处理
 * */
public class FlowPartition extends Partitioner<Text,FlowBean>{
   static HashMap<String, Integer> map = new HashMap<>();
    static {

        map.put("136",0);
        map.put("137",1);
        map.put("138",2);
        map.put("139",3);

    }
    @Override
    public int getPartition(Text text, FlowBean flowBean, int i) {
        String key = text.toString().substring(0, 3);
        Integer code = map.get(key);
        if (code == null){
            return 4;
        }
        return code;
    }
}
