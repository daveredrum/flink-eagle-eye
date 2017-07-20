package de.lmu.ifi.dbs.bigdatascience;

/**
 * Created by HuaWentao on 2017/6/21.
 */
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.Map;

public class DictionaryTest {


    @Before
    public void before() throws Exception {

        System.out.print("\nbefore\n\n");

    }

    @Test
    public void test() throws Exception {

        Dictionary dic = new Dictionary();
        Map<String, Integer> map = dic.train();
        System.out.print(map.get("java"));
        Assert.assertEquals("7477", map.get("java").toString());
        Map<String, Integer> mapr = dic.train();
        Assert.assertEquals("7477", mapr.get("java").toString());

    }
    @After
    public void after() throws Exception {

        System.out.print("\nfinish\n");

    }
}

