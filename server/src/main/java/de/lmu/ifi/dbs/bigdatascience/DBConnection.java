package de.lmu.ifi.dbs.bigdatascience;

import java.sql.PreparedStatement;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
/**
 * Created by HuaWentao on 2017/5/31.
 */
public class DBConnection {

    /** DB connection.  **/
    private Connection conn = null;

    /**  url to DB. **/
    private String url = null;

    /**  PreparedStatement. **/
    private PreparedStatement stmt = null;

    /**  amount of information.  **/
    private int num = 0;


    /**
     * initialize URL and  amount.  *
     * @param amount **amount of information**
     */
    public DBConnection(int amount) {
        url = "jdbc:mysql://localhost:3306/bigdata?"
                + "user=root&password=eagleeye&useUnicode=true&characterEncoding=UTF8";
        num = amount;
    }



    /**  set  PreparedStatement.
     * @param presql preprocess sql.*
     * @return the prepared statement**/
    public PreparedStatement getstatement(String presql) {
        try {

            final StringBuffer sql = new StringBuffer(presql);
            for (int i = 1; i < num; i++) {
                sql.append(" or Id = ?");
            }

            Class.forName("com.mysql.jdbc.Driver");
            conn = DriverManager.getConnection(url);
            stmt = conn.prepareStatement(sql.toString());
        } catch (SQLException e) {
            System.out.println("MySQL operation wrong");
            e.printStackTrace();
        } catch (Exception e) {
            e.printStackTrace();
        }
        return stmt;
    }

    /**   close connection. **/
    public void close() {
        try {
            conn.close();
            stmt.close();
        } catch (SQLException e) {
            System.out.println("MySQL操作错误");
            e.printStackTrace();
        }
    }




}
