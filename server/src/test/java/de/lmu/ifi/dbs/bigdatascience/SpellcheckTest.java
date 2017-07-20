package de.lmu.ifi.dbs.bigdatascience;

/**
 * Created by HuaWentao on 2017/6/21.
 */
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class SpellcheckTest {


    @Before
    public void before() throws Exception {

        System.out.print("\nbefore\n\n");

    }

    @Test
    public void test() throws Exception {

        System.out.print("\nbegin\n");
        Spellcheck s= new Spellcheck();
        System.out.print( s.correct("jaav")+"\n");
        Assert.assertEquals("java", s.correct("jaav"));
        Assert.assertEquals("java", s.correct("java"));



    }
    @After
    public void after() throws Exception {

        System.out.print("\nfinish\n");

    }
}

