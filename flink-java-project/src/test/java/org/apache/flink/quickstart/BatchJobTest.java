package org.apache.flink.quickstart;

import org.apache.flink.quickstart.BatchJob;
import org.junit.Test;

import static org.junit.Assert.*;

/**
 * Created by schmoll on 21.04.17.
 */
public class BatchJobTest {
    @org.junit.Before
    public void setUp() throws Exception {
    }

    @Test
    public void testTest() throws Exception {
        //throw new RuntimeException();
        WordCount.main(new String[0]);
        //SocketTextStreamWordCount.main(new String[0]);
        //StreamingJob.main(new String[0]);
    }

}
