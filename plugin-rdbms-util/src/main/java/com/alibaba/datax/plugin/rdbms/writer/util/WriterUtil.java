package com.alibaba.datax.plugin.rdbms.writer.util;

import com.alibaba.datax.common.exception.DataXException;
import com.alibaba.datax.common.util.Configuration;
import com.alibaba.datax.plugin.rdbms.util.DBUtil;
import com.alibaba.datax.plugin.rdbms.util.DBUtilErrorCode;
import com.alibaba.datax.plugin.rdbms.util.DataBaseType;
import com.alibaba.datax.plugin.rdbms.util.RdbmsException;
import com.alibaba.datax.plugin.rdbms.writer.Constant;
import com.alibaba.datax.plugin.rdbms.writer.Key;
import com.alibaba.druid.sql.parser.ParserException;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.Statement;
import java.util.*;
import java.util.stream.Collectors;

public final class WriterUtil {
    private static final Logger LOG = LoggerFactory.getLogger(WriterUtil.class);

    //TODO 切分报错
    public static List<Configuration> doSplit(Configuration simplifiedConf,
                                              int adviceNumber) {

        List<Configuration> splitResultConfigs = new ArrayList<Configuration>();

        int tableNumber = simplifiedConf.getInt(Constant.TABLE_NUMBER_MARK);

        //处理单表的情况
        if (tableNumber == 1) {
            //由于在之前的  master prepare 中已经把 table,jdbcUrl 提取出来，所以这里处理十分简单
            for (int j = 0; j < adviceNumber; j++) {
                splitResultConfigs.add(simplifiedConf.clone());
            }

            return splitResultConfigs;
        }

        if (tableNumber != adviceNumber) {
            throw DataXException.asDataXException(DBUtilErrorCode.CONF_ERROR,
                    String.format("您的配置文件中的列配置信息有误. 您要写入的目的端的表个数是:%s , 但是根据系统建议需要切分的份数是：%s. 请检查您的配置并作出修改.",
                            tableNumber, adviceNumber));
        }

        String jdbcUrl;
        List<String> preSqls = simplifiedConf.getList(Key.PRE_SQL, String.class);
        List<String> postSqls = simplifiedConf.getList(Key.POST_SQL, String.class);

        List<Object> conns = simplifiedConf.getList(Constant.CONN_MARK,
                Object.class);

        for (Object conn : conns) {
            Configuration sliceConfig = simplifiedConf.clone();

            Configuration connConf = Configuration.from(conn.toString());
            jdbcUrl = connConf.getString(Key.JDBC_URL);
            sliceConfig.set(Key.JDBC_URL, jdbcUrl);

            sliceConfig.remove(Constant.CONN_MARK);

            List<String> tables = connConf.getList(Key.TABLE, String.class);

            for (String table : tables) {
                Configuration tempSlice = sliceConfig.clone();
                tempSlice.set(Key.TABLE, table);
                tempSlice.set(Key.PRE_SQL, renderPreOrPostSqls(preSqls, table));
                tempSlice.set(Key.POST_SQL, renderPreOrPostSqls(postSqls, table));

                splitResultConfigs.add(tempSlice);
            }

        }

        return splitResultConfigs;
    }

    public static List<String> renderPreOrPostSqls(List<String> preOrPostSqls, String tableName) {
        if (null == preOrPostSqls) {
            return Collections.emptyList();
        }

        List<String> renderedSqls = new ArrayList<String>();
        for (String sql : preOrPostSqls) {
            //preSql为空时，不加入执行队列
            if (StringUtils.isNotBlank(sql)) {
                renderedSqls.add(sql.replace(Constant.TABLE_NAME_PLACEHOLDER, tableName));
            }
        }

        return renderedSqls;
    }

    public static void executeSqls(Connection conn, List<String> sqls, String basicMessage,DataBaseType dataBaseType) {
        Statement stmt = null;
        String currentSql = null;
        try {
            stmt = conn.createStatement();
            for (String sql : sqls) {
                currentSql = sql;
                DBUtil.executeSqlWithoutResultSet(stmt, sql);
            }
        } catch (Exception e) {
            throw RdbmsException.asQueryException(dataBaseType,e,currentSql,null,null);
        } finally {
            DBUtil.closeDBResources(null, stmt, null);
        }
    }

    public static String getWriteTemplate(List<String> columnHolders, List<String> valueHolders, String writeMode, DataBaseType dataBaseType, boolean forceUseUpdate) {
        boolean update = writeMode.trim().toLowerCase().startsWith("update");
        boolean isWriteModeLegal = writeMode.trim().toLowerCase().startsWith("insert")
                || writeMode.trim().toLowerCase().startsWith("replace")
                || writeMode.trim().toLowerCase().startsWith("update");

        if (!isWriteModeLegal) {
            throw DataXException.asDataXException(DBUtilErrorCode.ILLEGAL_VALUE,
                    String.format("您所配置的 writeMode:%s 错误. 因为DataX 目前仅支持replace,update 或 insert 方式. 请检查您的配置并作出修改.", writeMode));
        }
        // && writeMode.trim().toLowerCase().startsWith("replace")
        String writeDataSqlTemplate;
        if (forceUseUpdate ||
                ((dataBaseType == DataBaseType.MySql || dataBaseType == DataBaseType.Tddl) && writeMode.trim().toLowerCase().startsWith("update"))
                ) {
            //update只在mysql下使用

            writeDataSqlTemplate = new StringBuilder()
                    .append("INSERT INTO %s (").append(StringUtils.join(columnHolders, ","))
                    .append(") VALUES(").append(StringUtils.join(valueHolders, ","))
                    .append(")")
                    .append(onDuplicateKeyUpdateString(columnHolders))
                    .toString();
        } else if(dataBaseType == DataBaseType.Oracle && update){
                //支持oracle的更新
            String s1 = writeMode.trim().toLowerCase();
            String primaryKeys=s1.substring(s1.indexOf("(")+1,s1.indexOf(")")).replaceAll("\"","");

            String[] primaryKeyArrs = primaryKeys.split(",");
            String[] columns = columnHolders.stream().map(x -> x.replaceAll("\"", "")).toArray(String[]::new);
            writeDataSqlTemplate = getUpsertStatement("%s", columns, primaryKeyArrs, true);

        } else {

            //这里是保护,如果其他错误的使用了update,需要更换为replace
            if (writeMode.trim().toLowerCase().startsWith("update")) {
                writeMode = "replace";
            }
            writeDataSqlTemplate = new StringBuilder().append(writeMode)
                    .append(" INTO %s (").append(StringUtils.join(columnHolders, ","))
                    .append(") VALUES(").append(StringUtils.join(valueHolders, ","))
                    .append(")").toString();
        }
        System.out.println("==============:"+writeDataSqlTemplate);
        return writeDataSqlTemplate;
    }

    public static String onDuplicateKeyUpdateString(List<String> columnHolders){
        if (columnHolders == null || columnHolders.size() < 1) {
            return "";
        }
        StringBuilder sb = new StringBuilder();
        sb.append(" ON DUPLICATE KEY UPDATE ");
        boolean first = true;
        for(String column:columnHolders){
            if(!first){
                sb.append(",");
            }else{
                first = false;
            }
            sb.append(column);
            sb.append("=VALUES(");
            sb.append(column);
            sb.append(")");
        }

        return sb.toString();
    }

    public static void preCheckPrePareSQL(Configuration originalConfig, DataBaseType type) {
        List<Object> conns = originalConfig.getList(Constant.CONN_MARK, Object.class);
        Configuration connConf = Configuration.from(conns.get(0).toString());
        String table = connConf.getList(Key.TABLE, String.class).get(0);

        List<String> preSqls = originalConfig.getList(Key.PRE_SQL,
                String.class);
        List<String> renderedPreSqls = WriterUtil.renderPreOrPostSqls(
                preSqls, table);

        if (null != renderedPreSqls && !renderedPreSqls.isEmpty()) {
            LOG.info("Begin to preCheck preSqls:[{}].",
                    StringUtils.join(renderedPreSqls, ";"));
            for(String sql : renderedPreSqls) {
                try{
                    DBUtil.sqlValid(sql, type);
                }catch(ParserException e) {
                    throw RdbmsException.asPreSQLParserException(type,e,sql);
                }
            }
        }
    }

    public static void preCheckPostSQL(Configuration originalConfig, DataBaseType type) {
        List<Object> conns = originalConfig.getList(Constant.CONN_MARK, Object.class);
        Configuration connConf = Configuration.from(conns.get(0).toString());
        String table = connConf.getList(Key.TABLE, String.class).get(0);

        List<String> postSqls = originalConfig.getList(Key.POST_SQL,
                String.class);
        List<String> renderedPostSqls = WriterUtil.renderPreOrPostSqls(
                postSqls, table);
        if (null != renderedPostSqls && !renderedPostSqls.isEmpty()) {

            LOG.info("Begin to preCheck postSqls:[{}].",
                    StringUtils.join(renderedPostSqls, ";"));
            for(String sql : renderedPostSqls) {
                try{
                    DBUtil.sqlValid(sql, type);
                }catch(ParserException e){
                    throw RdbmsException.asPostSQLParserException(type,e,sql);
                }

            }
        }
    }

    public static void main(String[] args) {
        WriterUtil writerUtil = new WriterUtil();
        String user = writerUtil.getUpsertStatement("user", new String[]{"ID", "COL", "COL2"}, new String[]{"ID"}, true);
        System.out.println("转化前："+user);
        //wirtemode
        String wirtemode ="update(col1,col2)";
        String quStr=wirtemode.substring(wirtemode.indexOf("(")+1,wirtemode.indexOf(")"));

        System.out.println(quStr);


    }


    //oracle 拼接写法
    public static String  getUpsertStatement(String tableName, String[] fieldNames, String[] uniqueKeyFields, boolean allReplace) {
        StringBuilder mergeIntoSql = new StringBuilder();
        mergeIntoSql.append("MERGE INTO " + tableName + " T1 USING (").append(buildDualQueryStatement(fieldNames)).append(") T2 ON (").append(buildConnectionConditions(uniqueKeyFields) + ") ");
        String updateSql = buildUpdateConnection(fieldNames, uniqueKeyFields, allReplace);
        if (StringUtils.isNotEmpty(updateSql)) {
            mergeIntoSql.append(" WHEN MATCHED THEN UPDATE SET ");
            mergeIntoSql.append(updateSql);
        }

        mergeIntoSql.append(" WHEN NOT MATCHED THEN ").append("INSERT (").append((String)Arrays.stream(fieldNames).map((col) -> {
            return quoteIdentifier(col);
        }).collect(Collectors.joining(","))).append(") VALUES (").append((String)Arrays.stream(fieldNames).map((col) -> {
            return "T2." + quoteIdentifier(col);
        }).collect(Collectors.joining(","))).append(")");
        return parseNamedStatement(mergeIntoSql.toString(), new HashMap<>());
    }


    private static  String parseNamedStatement(String sql, Map<String, List<Integer>> paramMap) {
        StringBuilder parsedSql = new StringBuilder();
        int fieldIndex = 1;
        int length = sql.length();

        for(int i = 0; i < length; ++i) {
            char c = sql.charAt(i);
            if (':' != c) {
                parsedSql.append(c);
            } else {
                int j;
                for(j = i + 1; j < length && Character.isJavaIdentifierPart(sql.charAt(j)); ++j) {
                }

                String parameterName = sql.substring(i + 1, j);
                ((List)paramMap.computeIfAbsent(parameterName, (n) -> {
                    return new ArrayList();
                })).add(fieldIndex);
                ++fieldIndex;
                i = j - 1;
                parsedSql.append('?');
            }
        }
        return parsedSql.toString();
    }




    private static String buildDualQueryStatement(String[] column) {
        StringBuilder sb = new StringBuilder("SELECT ");
        String collect = (String)Arrays.stream(column).map((col) -> {
            return wrapperPlaceholder(col) + quoteIdentifier(col);
        }).collect(Collectors.joining(", "));
        sb.append(collect).append(" FROM DUAL");
        return sb.toString();
    }


    private static String buildUpdateConnection(String[] fieldNames, String[] uniqueKeyFields, boolean allReplace) {
        List<String> uniqueKeyList = Arrays.asList(uniqueKeyFields);
        String updateConnectionSql = (String)Arrays.stream(fieldNames).filter((col) -> {
            boolean bbool = !uniqueKeyList.contains(col.toLowerCase()) && !uniqueKeyList.contains(col.toUpperCase());
            return bbool;
        }).map((col) -> {
            return buildConnectionByAllReplace(allReplace, col);
        }).collect(Collectors.joining(","));
        return updateConnectionSql;
    }


    private static String quoteIdentifier(String identifier) {
        return "" + identifier + "";
    }

    private static String wrapperPlaceholder(String fieldName) {
        return " :" + fieldName + " ";
    }


    private static String buildConnectionByAllReplace(boolean allReplace, String col) {
        String conncetionSql = allReplace ? quoteIdentifier("T1") + "." + quoteIdentifier(col) + " = " + quoteIdentifier("T2") + "." + quoteIdentifier(col) : quoteIdentifier("T1") + "." + quoteIdentifier(col) + " =nvl(" + quoteIdentifier("T2") + "." + quoteIdentifier(col) + "," + quoteIdentifier("T1") + "." + quoteIdentifier(col) + ")";
        return conncetionSql;
    }


    private static String buildConnectionConditions(String[] uniqueKeyFields) {
        return (String)Arrays.stream(uniqueKeyFields).map((col) -> {
            return "T1." + quoteIdentifier(col.trim()) + "=T2." + quoteIdentifier(col.trim());
        }).collect(Collectors.joining(" and "));
    }





}
