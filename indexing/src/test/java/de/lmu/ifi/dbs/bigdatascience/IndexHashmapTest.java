package de.lmu.ifi.dbs.bigdatascience;

import java.io.File;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.junit.*;

/**
 * Created by Dave on 2017/5/31.
 */
public class IndexHashmapTest {

    @Test
    public void testGetIndexWithSomeParameter() throws Exception {
        String inputPath = "../data/input/questions.csv";
        File file = new File(inputPath);
        String abPath = file.getAbsolutePath();
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        IndexHashmap index = new IndexHashmap(env, abPath);
        index.getIndex();
        index.writeIndex();
        index.readIDF().print();
    }

}