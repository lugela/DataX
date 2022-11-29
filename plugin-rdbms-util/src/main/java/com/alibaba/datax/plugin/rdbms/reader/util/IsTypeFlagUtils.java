package com.alibaba.datax.plugin.rdbms.reader.util;

import com.alibaba.datax.common.exception.DataXException;
import com.alibaba.datax.plugin.rdbms.util.DBUtilErrorCode;

import java.sql.ResultSetMetaData;
import java.sql.Types;

/**
 * @description: 判断指定的类型
 * @author: lugela
 * @create: 2022-04-07 10:55
 */

public class IsTypeFlagUtils {


    public static boolean isPKTypeValid(ResultSetMetaData rsMetaData) {
        boolean ret = false;
        try {
            int minType = rsMetaData.getColumnType(1);
            int maxType = rsMetaData.getColumnType(2);

            boolean isNumberType = isLongType(minType);

            boolean isStringType = isStringType(minType);

            boolean isDecimal = isDecimal(minType);

            boolean isTime = isTime(minType);

            if (minType == maxType && (isNumberType || isStringType) || isDecimal ||  isTime) {
                ret = true;
            }
        } catch (Exception e) {
            throw DataXException.asDataXException(DBUtilErrorCode.ILLEGAL_SPLIT_PK,
                    "DataX获取切分主键(splitPk)字段类型失败. 该错误通常是系统底层异常导致. 请联系旺旺:askdatax或者DBA处理.");
        }
        return ret;
    }


    public static boolean isLongType(int type) {
        boolean isValidLongType = type == Types.BIGINT || type == Types.INTEGER
                || type == Types.SMALLINT || type == Types.TINYINT;

        return isValidLongType;
    }


    public static boolean isStringType(int type) {
        return type == Types.CHAR || type == Types.NCHAR
                || type == Types.VARCHAR || type == Types.LONGVARCHAR
                || type == Types.NVARCHAR;
    }


    public static boolean isDecimal(int type){
        return  type == Types.DECIMAL || type == Types.FLOAT || type == Types.REAL || type ==Types.DOUBLE || type == Types.NUMERIC;

    }

    public static boolean isTime(int type){
        return  type == Types.DATE || type == Types.TIME || type == Types.TIMESTAMP;
    }







}
