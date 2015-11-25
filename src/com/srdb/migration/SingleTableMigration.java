package com.srdb.migration;

import com.srdb.migration.metadata.Column;
import com.srdb.migration.metadata.Table;
import org.apache.log4j.Logger;
import org.srdbsql.Driver;

import java.sql.*;
import java.util.Properties;
import java.util.TimeZone;

/**
 * Created by kevin on 2015-11-19.
 */

public class SingleTableMigration {
    private static Logger logger = Logger.getLogger(SingleTableMigration.class.getName());
    public static Connection getSRConnection()  {
        Driver driver = new Driver();

        Properties prop = new Properties();
        prop.setProperty("user", "dba");
        prop.setProperty("password", "1");
        prop.setProperty("loginTimeout", "10");
        Connection conn = null;
        try {
            conn=driver.connect("jdbc:srdbsql://10.168.250.37:1975/srdb", prop);
        }catch (SQLException e){
            e.printStackTrace();
        }
        return conn;
    }
    public static Connection getOraConnection() {
        //定义一个连接对象
        TimeZone timeZone = TimeZone.getTimeZone("Asia/ShangHai");
        TimeZone.setDefault(timeZone);
        Connection conn = null;
        //定义连接数据库的URL资源
        String url = "jdbc:oracle:thin:@10.168.250.37:1521:ora11";
        //定义连接数据库的用户名称与密码
        String username = "HR";
        String password = "123456";
        //加载数据库连接驱动
        String className = "oracle.jdbc.driver.OracleDriver";
        try {
            Class.forName(className);
        } catch (ClassNotFoundException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
        //获取数据库的连接对象
        try {
            conn = DriverManager.getConnection(url, username, password);
            logger.debug("数据库连接建立成功...");
        } catch (SQLException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
        //返回连接对象
        return conn;
    }

    public static void main(String[] args) {
        //线程统计变量
        Params params = new Params();
        //设置迁移开始时间
        params.setTotalTime(System.currentTimeMillis());

        String tableName="WORKERS_INFO";
        String schema="HR";

        int threadNum=4;
        int pageNum=100000;
        if (args.length > 0 && args[0] != null) {
            threadNum = Integer.parseInt(args[0]);
        }
        if (args.length > 1 && args[1] != null) {
            pageNum = Integer.parseInt(args[1]);
        }
        if (args.length > 2 && args[2] != null) {
            tableName = args[2];

        }
        if (args.length > 3 && args[3] != null) {
            schema = args[3];
        }

        //开始迁移
        startload(threadNum, params, pageNum,schema,tableName);


    }

    /**
     * 线程创建并启动
     * @param threads 线程数
     * @param params 线程页面参数统计
     * @param pageNum 单页查询数据量
     * @param schema   模式
     * @param tableName 表名
     */
    public static void startload(int threads,Params params,int pageNum,String schema,String tableName) {
        //初始化表对象
        Table table=new Table(schema,tableName);
        Connection conn=getOraConnection();
        try {
            PreparedStatement getColumns = conn.prepareStatement(table.getGetColumnSqlStr());
            getColumns.setString(1, table.getParentSchema().toUpperCase());
            getColumns.setString(2, table.getName().toUpperCase());
            ResultSet rs = getColumns.executeQuery();
            while (rs.next()) {
                String data_default = rs.getString("data_default");
                String colName = rs.getString("column_name");
                Column column = new Column(table, colName);

                column.addDetail(rs.getString("data_type"),
                        rs.getInt("data_length"),
                        rs.getInt("data_precision"),
                        rs.getInt("data_scale"),
                        rs.getString("nullable"),
                        data_default,
                        rs.getString("comments"));
                table.addColumn(column);
            }
        }catch (SQLException e){
            e.printStackTrace();
        }
        //建模式
        String createSchemaSql=table.getCreateSchemaScript(table.getParentSchema());
        try{
            getSRConnection().createStatement().execute(createSchemaSql);
        }catch (SQLException e){
//            e.printStackTrace();
        }

        //建表
        String createTableSql=table.getCreateScript(table);
        try {
            getSRConnection().createStatement().execute(createTableSql);
            logger.info(table.getName()+" 表创建成功");

        }catch (SQLException e) {
            logger.info(table.getName()+" 表创建失败");

            e.printStackTrace();
            System.exit(-1);
        }

        logger.info("线程数： " + threads + "," +table.getName() + " 迁移开始 ... ");
        for (int i = 0; i < threads; i++) {
            Connection connSrc = getOraConnection();
            Connection connTarget = getSRConnection();
            if (connSrc == null || connTarget == null) {
                System.exit(0);
            }
            SingleTableLoader st = new SingleTableLoader(connSrc, connTarget, params, table, pageNum);
            String threadName = "线程_" + (i + 1);
            Thread t = new Thread(st);
            t.setName(threadName);
            t.start();
            try {
                Thread.sleep(100);
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }


}
