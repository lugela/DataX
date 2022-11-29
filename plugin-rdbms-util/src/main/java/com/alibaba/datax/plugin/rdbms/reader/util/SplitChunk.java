package com.alibaba.datax.plugin.rdbms.reader.util;

import com.alibaba.datax.common.util.Configuration;
import com.alibaba.datax.plugin.rdbms.reader.Constant;
import com.alibaba.datax.plugin.rdbms.reader.Key;
import com.alibaba.datax.plugin.rdbms.util.DBUtil;
import com.alibaba.datax.plugin.rdbms.util.DataBaseType;
import com.alibaba.datax.plugin.rdbms.util.RdbmsException;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

/**
 * @description: 1.0
 * @author: lugela
 * @create: 2022-04-04 20:37
 */

public class SplitChunk {


    private static final Logger LOG = LoggerFactory.getLogger(SplitChunk.class);


    public static long queryApproximateRowCnt(Configuration configuration, DataBaseType dataBaseType)
            {
        // The statement used to get approximate row count which is less
        // accurate than COUNT(*), but is more efficient for large table.
        ResultSet rs = null;
                int fetchSize = configuration.getInt(Constant.FETCH_SIZE);
        if (fetchSize <0) fetchSize =1024;
        String jdbcURL = configuration.getString(Key.JDBC_URL);
        String username = configuration.getString(Key.USERNAME);
        String password = configuration.getString(Key.PASSWORD);
        String table = configuration.getString(Key.TABLE);
        Connection conn = DBUtil.getConnection(dataBaseType, jdbcURL, username, password);

        final String rowCountQuery = String.format("SHOW TABLE STATUS LIKE '%s';", table);

        try {
            System.out.println(conn.getMetaData().getCatalogs());
            rs = DBUtil.query(conn, rowCountQuery, fetchSize);
            while (DBUtil.asyncResultSetNext(rs)) {
                if (rs.getMetaData().getColumnCount() < 5) {
                    System.out.println(rs.getMetaData());
                    throw new SQLException(
                            String.format(
                                    "No result returned after running query [%s]",
                                    rowCountQuery));
                }
                return rs.getLong(5);
            }
        } catch (Exception e) {
            throw RdbmsException.asQueryException(dataBaseType, e, rowCountQuery, table, username);
        }
        return 0;


    }

    public static String quote(String dbOrTableName) {
        return "`" + dbOrTableName + "`";
    }


    /**
     * Split table into unevenly sized chunks by continuously calculating next chunk max value.
     */
    public static List<String> splitUnevenlySizedChunks(
            Configuration configuration,
            Object min,
            Object max,
            int chunkSize,
            DataBaseType dataBaseType)
             {

        int fetchSize = configuration.getInt(Constant.FETCH_SIZE);
        String splitColumnName = configuration.getString(Key.SPLIT_PK);

        String jdbcURL = configuration.getString(Key.JDBC_URL);
        String username = configuration.getString(Key.USERNAME);
        String password = configuration.getString(Key.PASSWORD);
        String tableId = configuration.getString(Key.TABLE);
        String pkType = configuration.getString(Constant.PK_TYPE);

        Connection conn = DBUtil.getConnection(dataBaseType, jdbcURL, username, password);


        final List<String> splits = new ArrayList<>();
        Object chunkStart = min;
        try {
            Object chunkEnd = nextChunkEnd(conn, min, tableId, splitColumnName, max, chunkSize,fetchSize);
            int count = 0;
            while (chunkEnd != null && ObjectUtils.compare(chunkEnd, max) <= 0) {
                // we start from [null, min + chunk_size) and avoid [null, min)
                splits.add(splice_where( chunkStart, chunkEnd, splitColumnName, pkType));
                // may sleep a while to avoid DDOS on MySQL server
                maySleep(count++, tableId);
                chunkStart = chunkEnd;
                chunkEnd = nextChunkEnd(conn, chunkEnd, tableId, splitColumnName, max, chunkSize,fetchSize);
            }
        }catch (Exception e){
            LOG.info(e.getMessage());
        }

        // add the ending split
          splits.add(splice_where_end( chunkStart, max, splitColumnName, pkType));
        return splits;
    }

    //拼接sql
    private static String splice_where(Object chunkStart,Object chunkEnd,String pkName,String pkTpye){
        String where_sql = null;
        if (Constant.PK_TYPE_STRING.equals(pkTpye) || Constant.PK_TYPE_STRING.equals(pkTpye)){
            where_sql = String.format(" (%s%s%s <= %s AND %s < %s%s%s) ", "'", chunkStart,"'",pkName,pkName,"'", chunkEnd,"'");
        }

        if (Constant.PK_TYPE_DECIMAL.equals(pkTpye)){
            where_sql = String.format(" (%s%s%s <= %s AND %s < %s%s%s) ", " ", chunkStart," ",pkName,pkName," ", chunkEnd," ");
        }

        return where_sql;
    }

    private static String splice_where_end(Object chunkStart,Object chunkEnd,String pkName,String pkTpye){
        String where_sql = null;
        if (Constant.PK_TYPE_STRING.equals(pkTpye)){
            where_sql = String.format(" (%s%s%s <= %s AND %s <= %s%s%s) ", "'", chunkStart,"'",pkName,pkName,"'", chunkEnd,"'");

        }
        return where_sql;
    }


    private static void maySleep(int count, String tableId) {
        // every 100 queries to sleep 1s
        if (count % 10 == 0) {
            try {
                Thread.sleep(100);
            } catch (InterruptedException e) {
                // nothing to do
            }
            LOG.info("JdbcSourceChunkSplitter has split {} chunks for table {}", count, tableId);
        }
    }



    private static Object nextChunkEnd(
            Connection conn,
            Object previousChunkEnd,
            String tableId,
            String splitColumnName,
            Object max,
            int chunkSize,
            int fetchSize)
            throws SQLException {
        // chunk end might be null when max values are removed
        Object chunkEnd =
                queryNextChunkMax(conn, tableId, splitColumnName, chunkSize, previousChunkEnd,fetchSize);
        if (Objects.equals(previousChunkEnd, chunkEnd)) {
            // we don't allow equal chunk start and end,
            // should query the next one larger than chunkEnd
            chunkEnd = queryMin(conn, tableId, splitColumnName, chunkEnd);
        }
        if (ObjectUtils.compare(chunkEnd, max) >= 0) {
            return null;
        } else {
            return chunkEnd;
        }
    }


    private static Object queryNextChunkMax(
            Connection conn,
            String tableId,
            String splitColumnName,
            int chunkSize,
            Object includedLowerBound,
            int fetchSize)
            throws SQLException {
        String quotedColumn = quote(splitColumnName);
        String query =
                String.format(
                        "SELECT MAX(%s) FROM ("
                                + "SELECT %s FROM %s WHERE %s >= ? ORDER BY %s ASC LIMIT %s"
                                + ") AS T",
                        quotedColumn,
                        quotedColumn,
                        quote(tableId),
                        quotedColumn,
                        quotedColumn,
                        chunkSize);
        ResultSet rs = null;
        try {
            PreparedStatement ps = conn.prepareStatement(query);
            ps.setObject(1, includedLowerBound);
            rs = ps.executeQuery();
            while (DBUtil.asyncResultSetNext(rs)) {
         /*       if (!rs.next() ) {
                    throw new SQLException(
                            String.format(
                                    "No result returned after running query [%s]",
                                    query));
                }*/
                return rs.getObject(1);
            }
        } catch (Exception e) {
            e.printStackTrace();
        }

        return null;


    }



    private static Object queryMin(
            Connection conn,
            String tableId,
            String columnName,
            Object includedLowerBound)
            throws SQLException {
        final String minQuery =
                String.format(
                        "SELECT MIN(%s) FROM %s WHERE %s > ?",
                        quote(columnName), quote(tableId), quote(columnName));

        ResultSet rs = null;
        try {
            PreparedStatement ps = conn.prepareStatement(minQuery);
            ps.setObject(1, includedLowerBound);
            rs = ps.executeQuery();
            while (DBUtil.asyncResultSetNext(rs)) {
     /*           if (!rs.next() ) {
                    throw new SQLException(
                            String.format(
                                    "No result returned after running query [%s]",
                                    minQuery));
                }*/
                return rs.getObject(1);
            }
        } catch (Exception e) {
            e.printStackTrace();
        }

        return null;
    }


}


