import org.srdbsql.Driver;
import org.srdbsql.copy.CopyManager;
import org.srdbsql.core.BaseConnection;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Calendar;
import java.util.Properties;
import java.util.UUID;

/**
 * Created by kevin on 27/4/15.
 */

public class testSR {
    /**
     * SRDB连接创建
     *
     * @return
     * @throws SQLException
     */
    public static Connection getConnection() throws SQLException {
        Driver driver = new Driver();

        Properties prop = new Properties();
        prop.setProperty("user", "dba");
        prop.setProperty("password", "123456");
        prop.setProperty("loginTimeout", "10");
        return driver.connect("jdbc:srdbsql://10.168.250.37:1975/srdb", prop);
    }

    public static void main(String[] args) throws Exception {
        Connection conn = getConnection();
//        for (int i = 0; i <100 ; i++) {
//            insertInLageObj(conn);
//        }

//        copyOut(conn, "./test.csv", "csv", null, "t2");
//        copyIn(conn, "./test.csv", "csv", "t2_2");
//        createTempTab("tp", conn);
    }

    /**
     * 测试用
     *
     * @param conn
     * @throws Exception
     */
    public static void insertInLageObj(Connection conn) throws Exception {
        PreparedStatement ps = conn.prepareStatement("INSERT  INTO tp (add,name,pic) VALUES (?,?,?)");
        File file = new File("/Users/kevin/Downloads/aaad.bmp");
        FileInputStream fis = new FileInputStream(file);
        byte[] tmp = new byte[1024];
//        for (int i = 0; i < 100; i++) {
        ps.setTimestamp(1, new java.sql.Timestamp(Calendar.getInstance().getTime().getTime()));
        ps.setString(2, "redmax");
        ps.setBinaryStream(3, fis, file.length());
        ps.execute();
//        }

    }

    /**
     * 导出数据
     *
     * @param conn
     * @param outputFile
     * @param type
     * @param queryStr
     * @param tableName
     * @return
     * @throws Exception
     */
    public static long copyOut(Connection conn, String outputFile, String type, String queryStr, String tableName) throws Exception {
        StringBuffer sql = new StringBuffer("COPY (SELECT * FROM ");
        if (outputFile == null || outputFile.trim().isEmpty()) {
            throw new Exception("未指定导出文件");
        }
        if (tableName == null || tableName.trim().isEmpty()) {
            throw new Exception("未指定表名");
        } else {
            sql.append(tableName).append(" ");
        }
        if (queryStr != null && !queryStr.trim().isEmpty()) {
            sql.append("WHERE ").append(queryStr).append(" ");
        }
        sql.append(" ) TO STDOUT ");
        if (type != null && !type.trim().isEmpty() && type.trim().equalsIgnoreCase("csv")) {
            sql.append(" WITH ").append(type.toUpperCase());
        }

        System.out.println(sql.toString());
        long status = 0;
        CopyManager copyManager = new CopyManager((BaseConnection) conn);
//        FileWriter fw = new FileWriter(new File("./test.text"));
        FileOutputStream fileOutputStream = new FileOutputStream(new File(outputFile));
        status = copyManager.copyOut(sql.toString(), fileOutputStream);
        System.out.println("export data : " + status);
        return status;
    }

    /**
     * 导入数据
     *
     * @param conn
     * @param inputFile
     * @param type
     * @param tableName
     * @return
     * @throws Exception
     */
    public static long copyIn(Connection conn, String inputFile, String type, String tableName) throws Exception {
        StringBuffer sql = new StringBuffer("COPY ");
        if (inputFile == null || inputFile.trim().isEmpty()) {
            throw new Exception("未指定导入文件");
        }
        if (tableName == null || tableName.trim().isEmpty()) {
            throw new Exception("未指定导入的表");
        } else {
            sql.append(tableName).append(" FROM STDIN");
        }
        if (type != null && !type.trim().isEmpty() && type.trim().equalsIgnoreCase("csv")) {
            sql.append(" WITH ").append(type);
        }

        System.out.println(sql.toString());

        long status = 0;
        CopyManager copyManager = new CopyManager((BaseConnection) conn);
//        FileWriter fw = new FileWriter(new File("./test.text"));
        FileInputStream fileInputStream = new FileInputStream(new File(inputFile));
//        status = copyManager.copyIn("COPY tp from STDOUT with CSV ", fileInputStream);
        status = copyManager.copyIn(sql.toString(), fileInputStream);
        System.out.println("import data : " + status);
        return status;
    }

    /**
     * 创建临时表
     *
     * @param tableName
     * @param conn
     * @return
     * @throws SQLException
     */
    public static boolean createTempTab(String tableName, Connection conn) throws SQLException {
        boolean flag = false;
        String tmpTabName = "dump_tmp_" + UUID.randomUUID().toString().replace("-", "_");
        System.out.println(tmpTabName);
        Statement statement = conn.createStatement();
        System.out.println("create table " + tmpTabName + " as select * from " + tableName + " limit 1;");
        flag = statement.execute("create table " + tmpTabName + " as select * from " + tableName + " limit 1;");
        flag = flag ? statement.execute("truncate table " + tmpTabName) : flag;

        return flag;
    }


}
