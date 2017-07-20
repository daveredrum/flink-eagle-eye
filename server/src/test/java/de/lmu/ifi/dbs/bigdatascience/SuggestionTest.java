package de.lmu.ifi.dbs.bigdatascience;

/**
 * Created by HuaWentao on 2017/6/21.
 */

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.ArrayList;

public class SuggestionTest {

    String expected = "Python  Open Cv - Detecting multiple markers based on bright spots detection on raspberry pi 3";

    @Before
    public void before() throws Exception {

        System.out.print("\nbefore\n\n");

    }

    @Test
    public void test() throws Exception {
        Service s= new Service();
        ArrayList<Suggestion> list1 = s.suggestion("ja");
        ArrayList<Suggestion> list2 = s.suggestion("java t");
        ArrayList<Suggestion> list3 = s.suggestion("java to");

        Assert.assertEquals("javascript and", list1.get(1).getContent());
        Assert.assertEquals("java that is the", list2.get(1).getContent());
        Assert.assertEquals("java tool", list3.get(1).getContent());


    }
    @After
    public void after() throws Exception {
        System.out.print("\nfinish\n");
    }
}

