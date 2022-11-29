package com.alibaba.datax.plugin.rdbms.reader.util;

import com.alibaba.datax.common.util.Configuration;
import com.alibaba.datax.plugin.rdbms.reader.Constant;
import com.alibaba.datax.plugin.rdbms.reader.Key;
import com.alibaba.datax.plugin.rdbms.util.DBUtil;
import com.alibaba.datax.plugin.rdbms.util.DataBaseType;
import com.alibaba.datax.plugin.rdbms.util.RdbmsException;
import com.google.common.annotations.VisibleForTesting;
import org.apache.commons.lang3.tuple.Pair;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Date;

/**
 * @description:
 * @author: lugela
 * @create: 2022-04-05 11:21
 */

public class Test {



    public static void main(String[] args) {
        Date date = new Date();
        String  where_sql = String.format(" (%s%s%s <= %s AND %s < %s%s%s) ", " ", date," ","ID","ID"," ", date," ");
        System.out.println(where_sql);


     /*   String json ="{\n" +
                "\"fetchSize\":1024,\n" +
                "\"splitPk\":\"ID\",\n" +
                "\"jdbcUrl\":\"jdbc:mysql://172.17.65.219:3306/test_datax\"\t,\n" +
                "\"username\":\"root\",\n" +
                "\"password\":\"1qaz@WSX3edc\",\n" +
                "\"table\":\"LC_MainDataNew\"\n" +
                "}";
        Configuration configuration = new Configuration(json);
        Object mingPk = "10007678454300";
        Object maxPk = "9999080629300";
        long count =SplitChunk.queryApproximateRowCnt( configuration,  DataBaseType.MySql);
        int chunkSize = (int)(count / 10);
        //默认取8096
        chunkSize = Math.max(chunkSize,8096);
        SplitChunk.splitUnevenlySizedChunks(configuration, mingPk, maxPk, chunkSize, DataBaseType.MySql);*/
    }
}
