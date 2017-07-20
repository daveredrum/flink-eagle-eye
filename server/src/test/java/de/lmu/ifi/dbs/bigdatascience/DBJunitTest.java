package de.lmu.ifi.dbs.bigdatascience;

/**
 * Created by HuaWentao on 2017/6/9.
 */

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.sql.*;

public class DBJunitTest {

    /** DB connection.  **/
    private Connection conn = null;

    /**  url to DB. **/
    private String url = null;

    /**  PreparedStatement. **/
    private PreparedStatement stmt = null;

    ResultSet res = null;

    @Before
    public void before() throws Exception {

        System.out.print("before\n");
        url = "jdbc:mysql://localhost:3306/bigdata?"
                + "user=root&password=eagleeye&useUnicode=true&characterEncoding=UTF8";


        try {
            final String sql = "select * from question where Id = ?";
            Class.forName("com.mysql.jdbc.Driver");
            conn = DriverManager.getConnection(url);
            if(conn == null)
                return;
            stmt = conn.prepareStatement(sql);
        } catch (SQLException e) {
            System.out.println("MySQL operation wrong");
            e.printStackTrace();
        } catch (Exception e) {
            e.printStackTrace();
        }
        stmt.setInt(1,8588);
        res = stmt.executeQuery();


    }

    @Test
    public void test() throws Exception {

        DBConnection db=new DBConnection(1);
        PreparedStatement st = db.getstatement("select * from question where Id = ?");

        if(st != null)
            st.setInt(1,8588);
        ResultSet r = null;
        try {
            if (st != null)
                r = st.executeQuery();
            if(r != null && res != null) {
                if (r.next() && res.next())
                    Assert.assertEquals(r.getString(2), res.getString(2));
                else
                    Assert.assertEquals(1, 2);
            }
        } catch (SQLException e) {
            System.out.println("MySQL Opreation ");
            e.printStackTrace();
        }

         finally {
            try {
                if (r != null) {
                    r.close();
                }
                if (st != null) {
                    st.close();
                }
            } catch (Exception e2) {
                e2.printStackTrace();
            }
        }

        db.close();


    }
    @After
    public void after() throws Exception {

        System.out.print("after\n");
        stmt.close();
        conn.close();
    }
}
