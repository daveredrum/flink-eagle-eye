package de.lmu.ifi.dbs.bigdatascience;

/**
 * Created by HuaWentao on 2017/6/9.
 */

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.ArrayList;

public class ServiceJunitTest {

    String expected = "Python  Open Cv - Detecting multiple markers based on bright spots detection on raspberry pi 3";

    @Before
    public void before() throws Exception {

        System.out.print("before\n");

    }

    @Test
    public void test() throws Exception {

        Service se = new Service();
        ArrayList<Result> test_list = null;
        test_list = se.results("python");
        Assert.assertEquals(expected,test_list.get(10).getTitle());

    }
    @After
    public void after() throws Exception {

        System.out.print("after\n");

    }
}
