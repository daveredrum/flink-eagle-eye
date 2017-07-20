package de.lmu.ifi.dbs.bigdatascience;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import java.util.stream.Collectors;
import org.apache.commons.lang.StringUtils;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple1;
import org.apache.flink.api.java.tuple.Tuple10;
import org.apache.flink.api.java.tuple.Tuple11;
import static org.junit.Assert.assertTrue;
import org.junit.Test;

/**
 * Created by Dave on 2017/6/2.
 */
public class BMScoreTest {
    
    @Test
    public void rankingTest() throws Exception {
        /**perform a normal search in EE*/
        String query = "catch all index out of range errors in python";
        BMScore bm25 = new BMScore(query);
        List<String> ranking = bm25.computeScore();
        for (String item : ranking) {
            System.out.println(item);
        }

        int relDocs = ranking.size();
        System.out.println("the number of retrieved items is: " + relDocs);

        String[] googleStr = {"307805", "219320", "332017", "260207", "270891", "230060", "238896"
                , "131397", "275631", "264598"};
        List<String> google = new ArrayList<>();
        google.addAll(Arrays.asList(googleStr));
        //output the ranking comparison to a csv file

        /**output the result of new model*/
        FileWriter writer = new FileWriter("../data/rank_comparison/after_opt.csv");

        /**output the result of previous model*/
        //FileWriter writer = new FileWriter("../data/rank_comparison/before_opt.csv");

        for (String item : google) {
            writer.write(item + "," + google.indexOf(item) + "," + ranking.indexOf(item) + "\n");
        }
        writer.close();


        /**pre-process the data from google
         * perform several search in EE
         * */
        /*
        //read training data from local csv file
        String path = "C:\\Users\\Dave\\Desktop\\data.csv";
        BufferedReader br = new BufferedReader(new FileReader(path));

        String query = null;
        String line;
        String question;
        String docId;
        String[] row;
        List<Tuple11<String, Double, Double, Double, Double, Double, Double, Double, Double,
                        Double, Double>> ranking = null;
        while ((line = br.readLine()) != null) {
            row = line.split(",");
            question = row[0];
            docId = row[1];
            if (!question.equals(query)) {
                System.out.println("Searching: " + question);
                query = question;
                BMScoreAll bm25 = new BMScoreAll(query);
                ranking = bm25.computeScore();
                addNewLine(ranking, docId, query);
                System.out.println("Complete searching for " + question);
            }
            else {
                addNewLine(ranking, docId, query);
            }
        }
    }
    private void addNewLine(List<Tuple11<String, Double, Double, Double, Double, Double, Double, Double, Double,
            Double, Double>> ranking, String docId, String query) throws IOException {
        ArrayList<String> newLine = new ArrayList<>();
        boolean flag = false;
        for (Tuple11<String, Double, Double, Double, Double, Double, Double, Double, Double,
                Double, Double> item : ranking) {
            if (docId.equals(item.f0)) {
                flag = true;
                newLine.add(String.valueOf(item.f1));
                newLine.add(String.valueOf(item.f2));
                newLine.add(String.valueOf(item.f3));
                newLine.add(String.valueOf(item.f4));
                newLine.add(String.valueOf(item.f5));
                newLine.add(String.valueOf(item.f6));
                newLine.add(String.valueOf(item.f7));
                newLine.add(String.valueOf(item.f8));
                newLine.add(String.valueOf(item.f9));
                newLine.add(String.valueOf(item.f10));
            }
        }
        if (flag) {
            FileWriter writer = new FileWriter("C:\\Users\\Dave\\Desktop\\data_all.csv", true);
            String collect = newLine.stream().collect(Collectors.joining(","));
            writer.write(collect);
            writer.write("\n");
            writer.close();
        }
*/
    }
}