import org.apache.log4j.Logger;

import java.sql.*;
import java.util.ArrayList;


/**
 * Created by kevin on 2015/10/18.
 */
public class SingleTableLoader implements Runnable {
    private static Logger logger = Logger.getLogger(SingleTableLoader.class.getName());


    private Params rownum;
    private String tablName;
    private Connection connSrc;
    private Connection connTarget;
    private Statement st;
    private boolean endFlag;
    private int pageNum;

    int bachSize=10000;
    /**
     * @param connSrc    源库连接
     * @param connTarget 目标连接
     * @param rowNum     线程页数统计变量
     * @param tb         表名
     * @param pageNum    单页查询数据量
     */
    public SingleTableLoader(Connection connSrc, Connection connTarget, Params rowNum, String tb, int pageNum) {
        this.rownum = rowNum;
        this.tablName = tb;
        this.connSrc = connSrc;
        this.connTarget = connTarget;
        this.endFlag = true;
        this.pageNum = pageNum;
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

    public   void  getSrcData(Params rownum){
        String sql;
        ArrayList<PreparedStatement> sqlCache = new ArrayList<PreparedStatement>();
        synchronized (rownum) {
            sql = "select * from (select t.*,rownum rn from (select * from " + this.tablName + ") t ) where rn between " + rownum.getRowNum() + " and " + (rownum.getRowNum() + pageNum-1);// 分页查询
            rownum.setRowNum(rownum.getRowNum() + pageNum);
        }
        logger.debug(Thread.currentThread().getName()+": "+sql);//分页实际的sql
        /**
         * 迁移demo
         */
        try {
            this.st = this.connSrc.createStatement();
            ResultSet rs = st.executeQuery(sql);
            endFlag = false; //假设标示是false，当结果集有数据则改为真。否则线程终止。

            PreparedStatement ps = connTarget.prepareStatement("INSERT  INTO TEST ( id, idcard, workername, sex, salary, email, employeddates, department, age, location   ) VALUES ( ?, ?, ?, ?, ?, ?, ?, ?, ?, ?   )");
             int i = 0;//用于批量计算

            while (rs.next()) {
                ps.setLong(1, rs.getLong(1));                      //            id            | numeric
                ps.setLong(2, rs.getLong(2));                        //            idcard        | numeric
                ps.setString(3, rs.getString(3));                        //            workername    | character varying(20)
                ps.setString(4, rs.getString(4));                       //            sex           | character varying(5)
                ps.setLong(5, rs.getLong(5));                       //            salary        | numeric
                ps.setString(6, rs.getString(6));                       //            email         | character varying(30)
                ps.setString(7, rs.getString(7));                       //            employeddates | character varying(10)
                ps.setString(8, rs.getString(8));                       //            department    | character varying(30)
                ps.setLong(9, rs.getLong(9));                       //            age           | numeric
                ps.setString(10, rs.getString(10));                       //            location      | character varying(30)
                ps.addBatch();

                i++;
                if (i % bachSize == 0) {

                    //批量提交
                    ps.executeBatch();
                    ps.clearBatch();
                    synchronized (rownum) {
                        rownum.setTotalMg(rownum.getTotalMg() + bachSize);
                        logger.info(Thread.currentThread().getName() + ": 已迁移 " + rownum.getTotalMg() + " 行");

                    }
                    i = 0;

                }
                endFlag = true;
            }
            //最后不够批量的数据提交
            ps.executeBatch();
            ps.clearBatch();
            ps.close();
            synchronized (rownum) {
                rownum.setTotalMg(rownum.getTotalMg() + i);
            }

            if (!endFlag) {
                synchronized (rownum) {
                    logger.info(Thread.currentThread().getName() + ": 已迁移 " + rownum.getTotalMg() + " 行，耗时: "+ rownum.getTotalTime()/1000 +" 秒" );

                }

                logger.debug(Thread.currentThread().getName() + ": " + "迁移完成，连接关闭");
                connSrc.close();
                connTarget.close();
            }
        }catch (SQLException e){
            e.printStackTrace();
        }


    }

}
