package org.apache.hadoop.algorithm;

import org.apache.commons.io.FileUtils;

import java.io.File;
import java.io.IOException;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.sql.*;
import java.text.SimpleDateFormat;
import java.util.Date;

/**
 * Created by xiaoyue26 on 12/8/16.
 */
public class DebugClient {
    private static int lineno = 0;

    public synchronized static void print(String line) {
        try {
            File log = new File("/home/xiaoyue26/mylog");
            String data = FileUtils.readFileToString(log);
            data += lineno + ":" + line + "\n\n";
            FileUtils.write(log, data);
            lineno++;
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public synchronized static void print(String line, String logname) {
        try {
            File log = new File("/home/xiaoyue26/" + logname);
            String data = FileUtils.readFileToString(log);
            data += lineno + ":" + line + "\n\n";
            FileUtils.write(log, data);
            lineno++;
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public static void print(Throwable e) {
        try {
            File log = new File("/home/xiaoyue26/mylog");
            String data = FileUtils.readFileToString(log);
            String line = getExceptionInfo(e);
            data += lineno + ":" + line + "\n\n";
            FileUtils.write(log, data);
            lineno++;
        } catch (IOException ioe) {
            ioe.printStackTrace();
        }
    }

    private static String getExceptionInfo(Throwable e) {
        StringWriter sw = new StringWriter();
        PrintWriter pw = new PrintWriter(sw, true);
        e.printStackTrace(pw);
        pw.flush();
        sw.flush();
        return sw.toString();
    }

    public static String getArrayString(String[] array) {
        StringBuilder sb = new StringBuilder();
        for (String cur : array) {
            sb.append(cur + ",");
        }
        return sb.toString();
    }

    private static Connection getCon() {
        String url = "jdbc:mysql://xiaoyue26:3306/data?"
                + "user=root" +
                "&" +
                "password=mysql" +
                "&useUnicode=true&characterEncoding=UTF8";
        Connection conn = null;
        try {
            Class.forName("com.mysql.jdbc.Driver");
            conn = DriverManager.getConnection(url);
            return conn;
        } catch (ClassNotFoundException e) {
            DebugClient.print("driver error");
        } catch (SQLException e) {
            DebugClient.print("Get connection error");
        }
        return conn;
    }

    private static String dtString;
    static {
        Date dt = new Date();
        SimpleDateFormat matter1 = new SimpleDateFormat("yyyy-MM-dd");
        dtString=matter1.format(dt);
    }
    public static void rebuild(String before, String after) {
        Connection conn = getCon();
        String sql;
        try {
            DebugClient.print("driver success");
            sql = "insert into rebuild(dt,`before`,after) values(?,?,?)";
            PreparedStatement pstmt = conn.prepareStatement(sql);
            pstmt.setString(1, dtString);
            pstmt.setString(2, before);
            pstmt.setString(3, after);
            int result = pstmt.executeUpdate();
            if (result == -1) {
                DebugClient.print("insert error");
            }
        } catch (SQLException e) {
            DebugClient.print("Mysql op error:"+before+"=>"+after+"==>\n"+e.getMessage());
        } finally {
            try {
                conn.close();
            } catch (SQLException e) {
                DebugClient.print("close error");
            }
        }

    }
}
