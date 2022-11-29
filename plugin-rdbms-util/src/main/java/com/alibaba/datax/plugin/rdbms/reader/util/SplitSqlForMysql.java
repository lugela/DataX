package com.alibaba.datax.plugin.rdbms.reader.util;

import com.alibaba.datax.common.exception.DataXException;
import com.alibaba.datax.common.util.Configuration;
import com.alibaba.datax.plugin.rdbms.reader.Constant;
import com.alibaba.datax.plugin.rdbms.reader.Key;
import com.alibaba.datax.plugin.rdbms.util.*;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.Types;
import java.util.List;

/**
 * @description: 单独对mysql的 查询进行一个处理
 * @author: lugela
 * @create: 2022-04-07 10:16
 */

public class SplitSqlForMysql {
    private static final Logger LOG = LoggerFactory
            .getLogger(SplitSqlForMysql.class);

    // 得到线程结果集
    public static List<String> rangeList(Configuration configuration,Pair<Object, Object> minMaxPK,int adviceNum,DataBaseType DATABASE_TYPE){
        List<String> rangeList = null;
        String splitPkName = configuration.getString(Key.SPLIT_PK);

        String column = configuration.getString(Key.COLUMN);
        String table = configuration.getString(Key.TABLE);
        String where = configuration.getString(Key.WHERE, null);


        configuration.set(Key.QUERY_SQL, SingleTableSplitUtil.buildQuerySql(column, table, where));

        boolean isStringType = Constant.PK_TYPE_STRING.equals(configuration
                .getString(Constant.PK_TYPE));
        boolean isLongType = Constant.PK_TYPE_LONG.equals(configuration
                .getString(Constant.PK_TYPE));
        boolean isTimeType = Constant.PK_TYPE_TIME.equals(configuration
                .getString(Constant.PK_TYPE));
        boolean isDecimalType = Constant.PK_TYPE_DECIMAL.equals(configuration
                .getString(Constant.PK_TYPE));

        if (isLongType){
            rangeList = RdbmsRangeSplitWrap.splitAndWrap(
                    String.valueOf(minMaxPK.getLeft()),
                    String.valueOf(minMaxPK.getRight()), adviceNum,
                    splitPkName, "'", DATABASE_TYPE);
        }else if(isStringType || isTimeType || isDecimalType){
            //mysql
            Object mingPk =minMaxPK.getLeft();
            Object maxPk = minMaxPK.getRight();
            long count =SplitChunk.queryApproximateRowCnt( configuration,  DATABASE_TYPE);
            int chunkSize = (int)(count / adviceNum);
            //默认取8096
            chunkSize = Math.max(chunkSize,8096);
            rangeList = SplitChunk.splitUnevenlySizedChunks(configuration, mingPk, maxPk, chunkSize, DATABASE_TYPE);

        }else{
            throw DataXException.asDataXException(DBUtilErrorCode.ILLEGAL_SPLIT_PK,
                    "根据切分主键切分表失败. DataX 仅支持切分主键为一个,切分字段类型请参考文档. 请尝试使用其他的切分主键或者联系 DBA 进行处理.");
        }



        return rangeList;
    }




    public static Pair<Object, Object> getMinMaxPK(Configuration configuration,DataBaseType DATABASE_TYPE){
        String pkRangeSQL = SingleTableSplitUtil.genPKRangeSQL(configuration);
        int fetchSize = configuration.getInt(Constant.FETCH_SIZE);
        String jdbcURL = configuration.getString(Key.JDBC_URL);
        String username = configuration.getString(Key.USERNAME);
        String password = configuration.getString(Key.PASSWORD);
        String table = configuration.getString(Key.TABLE);
        Connection conn = DBUtil.getConnection(DataBaseType.MySql, jdbcURL, username, password);

        LOG.info("split pk [sql={}] is running... ", pkRangeSQL);
        ResultSet rs = null;

        Pair<Object, Object> minMaxPK = null;

        try {
            rs = DBUtil.query(conn, pkRangeSQL, fetchSize);
            ResultSetMetaData rsMetaData = rs.getMetaData();
            if (IsTypeFlagUtils.isPKTypeValid(rsMetaData)) {
                boolean flag = false;
                if (configuration != null) {
                    //判断是否存在切分类型
                    if (IsTypeFlagUtils.isStringType(rsMetaData.getColumnType(1))) {
                        configuration.set(Constant.PK_TYPE, Constant.PK_TYPE_STRING);
                        flag = true;
                    } else if (IsTypeFlagUtils.isLongType(rsMetaData.getColumnType(1))) {
                        configuration.set(Constant.PK_TYPE, Constant.PK_TYPE_LONG);
                        flag = true;
                    } else if (IsTypeFlagUtils.isDecimal(rsMetaData.getColumnType(1))) {
                        configuration.set(Constant.PK_TYPE, Constant.PK_TYPE_DECIMAL);
                        flag = true;
                    } else if (IsTypeFlagUtils.isTime(rsMetaData.getColumnType(1))) {
                        configuration.set(Constant.PK_TYPE, Constant.PK_TYPE_TIME);
                        flag = true;
                    } else {
                        throw DataXException.asDataXException(DBUtilErrorCode.ILLEGAL_SPLIT_PK,
                                "您配置的DataX切分主键(splitPk)有误. 因为您配置的切分主键(splitPk) 类型 DataX 不支持. DataX 仅支持切分主键为一个,参考文档查看支持的类型. 请尝试使用其他的切分主键或者联系 DBA 进行处理.");
                    }

                } else {
                    //JDBC_NULL
                    throw DataXException.asDataXException(DBUtilErrorCode.JDBC_NULL,
                            "您配置的参数不合法，请检查配置文件。。。。。");
                }

                if (flag) {
                    while (DBUtil.asyncResultSetNext(rs)) {
                        minMaxPK = new ImmutablePair<Object, Object>(rs.getString(1), rs.getString(2));
                    }
                }

            }else {
                throw DataXException.asDataXException(DBUtilErrorCode.MYSQL_SPLIT_PK_ERROR,
                        "您配置的配做的主键切分类型不合法，请参考文档后进行处理。。。。。");
            }

        }catch (Exception e) {
            throw RdbmsException.asQueryException(DATABASE_TYPE, e, pkRangeSQL,table,username);
        }

        return minMaxPK;

    }









}
