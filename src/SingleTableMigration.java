import org.apache.log4j.Logger;
import org.srdbsql.Driver;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
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
        Params params = new Params();
        params.setTotalTime(System.currentTimeMillis());
        String tableName="WORKERS_INFO";
//        String tableName="T1";//test
        int threadNum=4;
        int pageNum=100000;
        if (args.length!=0 &&args[0]!=null){
            threadNum=Integer.parseInt(args[0]);
        }
        if (args.length!=0 &&args[1]!=null) {
            pageNum = Integer.parseInt(args[1]);
        }
        startload(threadNum, params, pageNum, tableName);


    }

    /**
     * 线程创建并启动
     * @param threads 线程数
     * @param params 线程页面参数统计
     * @param pageNum 单页查询数据量
     * @param tableName 表名
     */
    public static void startload(int threads,Params params,int pageNum,String tableName) {
        logger.info("线程数： " + threads + "," + tableName + "迁移开始");
        for (int i = 0; i < threads; i++) {
            Connection connSrc = getOraConnection();
            Connection connTarget = getSRConnection();
            if (connSrc == null || connTarget == null) {
                System.exit(0);
            }
            SingleTableLoader st = new SingleTableLoader(connSrc, connTarget, params, tableName, pageNum);
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
