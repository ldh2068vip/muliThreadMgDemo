package com.srdb.migration;

import com.srdb.migration.metadata.Column;
import com.srdb.migration.metadata.Table;
import org.apache.log4j.Logger;
import org.srdbsql.util.SRInterval;
import org.srdbsql.util.SRmoney;

import java.io.*;
import java.sql.*;
import java.util.ArrayList;


/**
 * Created by kevin on 2015/10/18.
 */
public class SingleTableLoader implements Runnable {
    private static Logger logger = Logger.getLogger(SingleTableLoader.class.getName());


    private Params rownum;
    private Connection connSrc;
    private Connection connTarget;
    private Statement st;
    private boolean endFlag;
    private int pageNum;
    private Table table;
    private PreparedStatement ps;
    private String insertStr;
    private int batchSize=10000;

    /**
     * @param connSrc    源库连接
     * @param connTarget 目标连接
     * @param rowNum     线程页数统计变量
     * @param table      表对象
     * @param pageNum    单页查询数据量
     */
    public SingleTableLoader(Connection connSrc, Connection connTarget, Params rowNum, Table table, int pageNum) {
        this.rownum = rowNum;
        this.connSrc = connSrc;
        this.connTarget = connTarget;
        this.endFlag = true;
        this.pageNum = pageNum;
        this.table = table;

        this.insertStr = "INSERT INTO " + table.getTargetSchemaQualifiedName()
                + " VALUES (";

        for (int i = 0; i < table.getColumns().size(); i++) {

            this.insertStr = this.insertStr + "?";
            if (i < table.getColumns().size() - 1)
                this.insertStr = this.insertStr + ", ";
        }
        this.insertStr = this.insertStr + ");";

        logger.debug("insert 语句 :" + this.insertStr);
        try {
            this.ps = connTarget.prepareStatement(this.insertStr);

        }catch (SQLException e) {
            e.printStackTrace();
        }

    }


    @Override
    public void run() {

        while (endFlag) {
            try {
                getSrcData(rownum);
//                Thread.sleep(50);
            } catch (Exception e) {
                e.printStackTrace();
            }
        }

    }

    public   void  getSrcData(Params rownum) throws  Exception {
        ByteArrayInputStream[] arrBis = new ByteArrayInputStream[table
                .getLargeObjectsColumnCount() * batchSize];
        String sql;
        int bisIndex = 0;
        ArrayList<PreparedStatement> sqlCache = new ArrayList<PreparedStatement>();
        synchronized (rownum) {
            sql = "select * from (select t.*,rownum rn from (select * from " + table.getParentSchema() + "." + table.getName() + ") t ) where rn between " + rownum.getRowNum() + " and " + (rownum.getRowNum() + pageNum - 1);// 分页查询
            rownum.setRowNum(rownum.getRowNum() + pageNum);
        }
        logger.debug(Thread.currentThread().getName() + ": " + sql);//分页实际的sql
        /**
         * 迁移主体
         */
        try {
            this.st = this.connSrc.createStatement();
            this.connTarget.setAutoCommit(false);
            ResultSet rs = st.executeQuery(sql);
            endFlag = false; //假设标示是false，当结果集有数据则改为真。否则线程终止。

            int m = 0;//用于批量计算

            while (rs.next()) {
                /**
                 *类型转换
                 */
                for (int i = 0; i < table.getColumns().size(); i++) {
                    Column column = table.getColumns().get(i);
//                    logger.debug("current type value index: " + table.getColumns().get(i).getDataType());
//                    logger.debug("current type value : " + table.getColumns().get(i).getDataType().ordinal());
                    switch (Column.DataType.values()[table.getColumns().get(i)
                            .getDataType().ordinal()]) {
                        //
                        case DATE:
                            ps.setDate(i + 1, rs.getDate(i + 1));
                            break;
                        case VARCHAR:
                        case NVARCHAR:
                        case TEXT:
                        case XML:
                            ps.setString(i + 1, rs.getString(i + 1));
                            break;
                        case BINARY_FLOAT:
                            if (rs.getObject(i + 1) != null)
                                ps.setDouble(i + 1, rs.getDouble(i + 1));
                            else {
                                ps.setNull(i + 1, 6);
                            }

                            break;
                        case NUMERIC://
                        case FLOAT:
                        case INTEGER:
                            Object objVal = rs.getObject(i + 1);

                            if (column.isReal()) {
                                if (objVal != null)
                                    ps.setFloat(i + 1, rs.getFloat(i + 1));
                                else
                                    ps.setNull(i + 1, 6);
                            } else if (column.isDouble()) {
                                if (objVal != null)
                                    ps.setDouble(i + 1, rs.getFloat(i + 1));
                                else {
                                    ps.setNull(i + 1, 8);
                                }
                            } else if (objVal != null)
                                ps.setBigDecimal(i + 1,
                                        rs.getBigDecimal(i + 1));
                            else {
                                ps.setNull(i + 1, 2);
                            }

                            break;
                        case TIME:
                        case TIMESTAMP:
                            ps.setTimestamp(i + 1, rs.getTimestamp(i + 1));
                            break;
                        case BYTEA:// blob等大字段处理
                            byte[] b = rs.getBytes(i + 1);//long  存pdf、bmp时，类型Io异常 类型长度大于最大值
                            if (b != null) {
//                                    this.lastMigratedDataSize += b.length;
                                arrBis[bisIndex] = new ByteArrayInputStream(b);
                                ps.setBinaryStream(i + 1,
                                        arrBis[(bisIndex++)], b.length);
                            } else {
                                ps.setNull(i + 1, -2);
                            }

                            break;
                        case INTERVAL:
                            SRInterval pgInterval = null;
                            ps.setObject(i + 1, pgInterval);
                            break;
                        case BOOLEAN:
                            ps.setBoolean(i + 1, rs.getBoolean(i + 1));
                            break;
                        case MONEY:
                            ps.setObject(i + 1,
                                    new SRmoney(rs.getString(i + 1)));
                            break;
                        case USERDEFINED:
                        case NAME:
                        case OID:
                        case GUID:
                        case ROWID:
                        case NETWORKADDRESS:
                            ps.setObject(i + 1, rs.getString(i + 1), 1111);
                            break;
                        case ARRAY:
                            ps.setArray(i + 1, rs.getArray(i + 1));
                            break;
                        case GEOMETRIC:
                            ps.setObject(i + 1, rs.getObject(i + 1));
                            break;
                        default:
                            throw new Exception("类型不支持");
                    }
                }

                ps.addBatch();

                m++;
                if (m % batchSize == 0) {

                    //批量提交
                    ps.executeBatch();
                    ps.clearBatch();
                    connTarget.commit();
                    synchronized (rownum) {
                        rownum.setTotalMg(rownum.getTotalMg() + batchSize);
                        logger.info(Thread.currentThread().getName() + ": 已迁移 " + rownum.getTotalMg() + " 行");

                    }
                    m = 0;

                }
                endFlag = true;
            }
            //最后不够批量的数据提交
            ps.executeBatch();
            ps.clearBatch();
            connTarget.commit();

            synchronized (rownum) {
                rownum.setTotalMg(rownum.getTotalMg() + m);
            }

            if (!endFlag) {
                synchronized (rownum) {
                    logger.info(Thread.currentThread().getName() + ": 已迁移 " + rownum.getTotalMg() + " 行，耗时: " + rownum.getTotalTime() / 1000 + " 秒");

                }

                logger.debug(Thread.currentThread().getName() + ": " + "迁移完成，连接关闭");
                connSrc.close();
                connTarget.close();
                ps.close();
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }


    }

}
