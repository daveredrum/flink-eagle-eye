// CHECKSTYLE:ON
package de.lmu.ifi.dbs.bigdatascience;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import org.apache.commons.math3.analysis.function.Max;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.operators.Order;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple1;
import org.apache.flink.api.java.tuple.Tuple10;
import org.apache.flink.api.java.tuple.Tuple11;
import org.apache.flink.api.java.tuple.Tuple12;
import org.apache.flink.api.java.tuple.Tuple13;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.api.java.tuple.Tuple5;
import org.apache.flink.api.java.tuple.Tuple6;
import org.apache.flink.api.java.tuple.Tuple8;
import org.apache.flink.api.java.tuple.Tuple9;

/**
 * Created by Dave on 2017/5/31.
 */

/**Calculate the BM25 score and return the ranking
 * along with all the features:
 * <ID, viewedCount, favoriteCount, commentCount, answerCount, span, closed, score>.*/
public class BMScoreAll {
    /**free parameter.*/
    private static final double FREE_PARAMETER_K = 1.5;
    /**free parameter.*/
    private static final double FREE_PARAMETER_B = 0.75;
    /**free parameter for optimization*/
    private static final double[] FREE_PARAMETER_P = {1.0, 1.0, 1.0, 0, 0, 0, 0, 1.0};
    /**the average length of docs.*/
    private double avgDocLength;
    /**the query sentence saved as a string array.*/
    private ArrayList<String> query = new ArrayList<>();
    /**IDF calculated in IndexHashMap.*/
    private DataSet<Tuple2<String, Double>> inputIDF;
    /**TF calculated in IndexHashMap.*/
    private DataSet<Tuple4<String, String, Integer, Double>> inputTF;
    /**more features: <ID, viewedCount, favoriteCount, commentCount, answerCount, span, closed, score>*/
    private DataSet<Tuple8<String, Integer, Integer, Integer, Integer, Integer, Integer, Integer>> features;
    /**max values for normalization*/
    private Tuple6<Integer, Integer, Integer, Integer, Integer, Integer> maxValueInFeatures;
    /**min values for normalization*/
    private Tuple6<Integer, Integer, Integer, Integer, Integer, Integer> minValueInFeatures;

    /**initialize the environment and split the query sentence.
     * @param query the query sentence
     * @throws Exception if there are any*/
    public BMScoreAll(String query) throws Exception {
        String inputPath = "../data/input/questions.csv";
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        env.getConfig().disableSysoutLogging();
        IndexHashmap index = new IndexHashmap(env, inputPath);
        this.avgDocLength = index.getDocAvgLength();

        //this.query.addAll(Arrays.asList(query.split("\\W|\\s")));

        //with word stemming
        this.query = new ArrayList<>();
        Stemmer wordStemmer = new Stemmer();
        for (String word : query.split("\\W|\\s")) {
            for (char ch : word.toLowerCase().toCharArray()) {
                wordStemmer.add(ch);
            }
            wordStemmer.stem();
            this.query.add(wordStemmer.toString());
        }

        inputIDF = index.readIDF();
        inputTF = index.readTF();

        //<ID, viewedCount, favoriteCount, commentCount, answerCount, span, closed, score>
        features = env
                .readCsvFile("../data/input/question_to_model.csv")
                .lineDelimiter("\n")
                .fieldDelimiter(",")
                .types(String.class, String.class, String.class, String.class,
                        String.class, String.class, String.class, String.class)
                .map(new valueMapper());

        //<viewedCount, favoriteCount, commentCount, answerCount, closed, score>
        maxValueInFeatures = new Tuple6<>(
                features.maxBy(1).project(1).collect().get(0).getField(0),
                features.maxBy(2).project(2).collect().get(0).getField(0),
                features.maxBy(3).project(3).collect().get(0).getField(0),
                features.maxBy(4).project(4).collect().get(0).getField(0),
                features.maxBy(6).project(6).collect().get(0).getField(0),
                features.maxBy(7).project(7).collect().get(0).getField(0)
        );
        minValueInFeatures = new Tuple6<>(
                features.minBy(1).project(1).collect().get(0).getField(0),
                features.minBy(2).project(2).collect().get(0).getField(0),
                features.minBy(3).project(3).collect().get(0).getField(0),
                features.minBy(4).project(4).collect().get(0).getField(0),
                features.minBy(6).project(6).collect().get(0).getField(0),
                features.minBy(7).project(7).collect().get(0).getField(0)
        );

    }

    /**compute the score.
     * @throws Exception if there are any
     * @return the list of ranking*/
    public List<Tuple11<String, Double, Double, Double, Double, Double, Double, Double, Double, Double, Double>>
    computeScore() throws Exception {
        //List<Tuple9<String, Double, Double, Double, Double, Double, Double, Double, Double>> result = new ArrayList<>();
        //<word, docID, docLength, idf, tf>
        DataSet<Tuple5<String, String, Integer, Double, Double>> jointIndex
                = inputIDF.join(inputTF).where(0).equalTo(0)
                .map(new IndexConnector());

        //initialize data set for the first iteration: <docID, BM25>
        DataSet<Tuple2<String, Double>> temp = jointIndex
                .filter(new QueryFilter(query.get(0)))
                .map(new FractionMapper(avgDocLength));
        for (String word : query) {
            DataSet<Tuple2<String, Double>> score = jointIndex
                    .filter(new QueryFilter(word))
                    .map(new FractionMapper(avgDocLength));
            temp = temp.union(score);
        }

        //get the max and min value for normalization
        double maxOfBM25 = temp.max(1).project(1).collect().get(0).getField(0);
        double minOfBM25 = temp.min(1).project(1).collect().get(0).getField(0);

        //map to the normalized data set and calculate the score
        DataSet<Tuple10<String, Double, Double, Double, Double, Double, Double, Double, Double, Double>> score = temp
                .groupBy(0).sum(1)
                .join(features).where(0)
                .equalTo(0).map(new scoreMapper(maxOfBM25, minOfBM25, maxValueInFeatures, minValueInFeatures));

        DataSet<Tuple11<String, Double, Double, Double, Double, Double, Double,
                Double, Double, Double, Double>> result = score
                .sortPartition(1, Order.DESCENDING)
                .map(new finalMapper(
                    score.maxBy(1).project(1).collect().get(0).getField(0)
        ));

        return result.collect();
    }

    /**map IDF and TF together.*/
    private static class IndexConnector implements MapFunction<Tuple2<Tuple2<String, Double>,
            Tuple4<String, String, Integer, Double>>, Tuple5<String, String, Integer, Double, Double>> {
        /** Serial version. */
        public static final long serialVersionUID = 1L;
        @Override
        public Tuple5<String, String, Integer, Double, Double> map(Tuple2<Tuple2<String, Double>,
                Tuple4<String, String, Integer, Double>> value) throws Exception {

            //with word stemming
            Stemmer wordStemmer = new Stemmer();
            for (char ch : value.f0.f0.toCharArray()) {
                wordStemmer.add(ch);
            }
            wordStemmer.stem();
            String word = wordStemmer.toString();

            //String word = value.f0.f0;

            String doc = value.f1.f1;
            Integer docLength = value.f1.f2;
            Double idf = value.f0.f1;
            Double tf = value.f1.f3;
            return new Tuple5<>(word, doc, docLength, idf, tf);
        }
    }

    /**get data set with the required word.*/
    private static class QueryFilter implements FilterFunction<Tuple5<String, String, Integer, Double, Double>> {
        /** Serial version. */
        public static final long serialVersionUID = 1L;
        /**query word.*/
        private String word;
        /**initialize query word.
         * @param word query word*/
        QueryFilter(String word) {
            this.word = word;
        }

        @Override
        public boolean filter(Tuple5<String, String, Integer, Double, Double> value) throws Exception {
            return value.f0.equals(word);
        }
    }

    /**map the data set with the score of each doc.*/
    private static class FractionMapper implements MapFunction<Tuple5<String, String, Integer, Double, Double>,
            Tuple2<String, Double>> {
        /** Serial version. */
        public static final long serialVersionUID = 1L;
        /** avgDocLength.*/
        private double avgDocLength;
        /** initialize the avgDocLength.
         * @param avgDocLength the average length of doc*/
        FractionMapper(double avgDocLength) {
            this.avgDocLength = avgDocLength;
        }
        /** map.*/
        @Override
        public Tuple2<String, Double> map(Tuple5<String, String, Integer, Double, Double> value)
                throws Exception {
            double result =  value.f2 * value.f3
                    * (FREE_PARAMETER_K + 1) / (value.f3 + FREE_PARAMETER_K
                    * (1 - FREE_PARAMETER_B + FREE_PARAMETER_B * value.f2 / avgDocLength));
            return new Tuple2<>(value.f1, result);
        }
    }

    private class valueMapper implements MapFunction<Tuple8<String, String, String, String,
            String, String, String, String>, Tuple8<String, Integer, Integer,
            Integer, Integer, Integer, Integer, Integer>> {
        @Override
        public Tuple8<String, Integer, Integer, Integer, Integer, Integer, Integer, Integer>
        map(Tuple8<String, String, String, String, String, String, String, String> value) throws Exception {
            String docID = value.f0;
            Integer viewedCount = Integer.parseInt(value.f1);
            Integer favoriteCount = Integer.parseInt(value.f2);
            Integer commentCount = Integer.parseInt(value.f3);
            Integer answerCount = Integer.parseInt(value.f4);
            Integer span = Integer.parseInt(value.f5);
            Integer closed = Integer.parseInt(value.f6);
            Integer score = Integer.parseInt(value.f7);
            return new Tuple8<>(docID, viewedCount, favoriteCount, commentCount, answerCount, span, closed, score);
        }
    }

    private static class scoreMapper implements MapFunction<Tuple2<Tuple2<String, Double>,
            Tuple8<String, Integer, Integer, Integer, Integer, Integer, Integer, Integer>>, Tuple10<String, Double,
            Double, Double, Double, Double, Double, Double, Double, Double>> {
        /**max value of BM25*/
        double max;
        /**min value of BM25*/
        double min;
        /**max values for normalization*/
        private Tuple6<Integer, Integer, Integer, Integer, Integer, Integer> maxValueInFeatures;
        /**min values for normalization*/
        private Tuple6<Integer, Integer, Integer, Integer, Integer, Integer> minValueInFeatures;

        scoreMapper(double max,
                    double min,
                    Tuple6<Integer, Integer, Integer, Integer, Integer, Integer> maxValueInFeatures,
                    Tuple6<Integer, Integer, Integer, Integer, Integer, Integer> minValueInFeatures) {
            this.max = max;
            this.min = min;
            this.maxValueInFeatures = maxValueInFeatures;
            this.minValueInFeatures = minValueInFeatures;
        }
        /**more features: <ID, viewedCount, favoriteCount, commentCount, answerCount, span, closed, score>*/
        @Override
        public Tuple10<String, Double, Double, Double, Double, Double, Double, Double, Double, Double> map(Tuple2<Tuple2<String,
                Double>, Tuple8<String, Integer, Integer, Integer, Integer, Integer, Integer, Integer>> value)
                throws Exception {
            double bm25 = (value.f0.f1 - min) / (max - min);
            double viewedCount = (double)(value.f1.f1 - minValueInFeatures.f0) / (maxValueInFeatures.f0 - minValueInFeatures.f0);
            double favoriteCount = (double)(value.f1.f2 - minValueInFeatures.f1) / (maxValueInFeatures.f1 - minValueInFeatures.f1);
            double commentCount = (double)(value.f1.f3 - minValueInFeatures.f2) / (maxValueInFeatures.f2 - minValueInFeatures.f2);
            double answerCount = (double)(value.f1.f4 - minValueInFeatures.f3) / (maxValueInFeatures.f3 - minValueInFeatures.f3);
            double span = (double)(value.f1.f5 - minValueInFeatures.f4) / (maxValueInFeatures.f4 - minValueInFeatures.f4);
            double score = (double)(value.f1.f7 - minValueInFeatures.f5) / (maxValueInFeatures.f5 - minValueInFeatures.f5);

            /*double bm25 = value.f0.f1 - min;
            double viewedCount = value.f1.f1;
            double favoriteCount = value.f1.f2;
            double commentCount = value.f1.f3;
            double answerCount = value.f1.f4;
            double span = value.f1.f5;
            double score = value.f1.f7;*/

            double result = FREE_PARAMETER_P[0] * bm25
                    + FREE_PARAMETER_P[1] * viewedCount
                    + FREE_PARAMETER_P[2] * favoriteCount
                    + FREE_PARAMETER_P[3] * commentCount
                    + FREE_PARAMETER_P[4] * answerCount
                    + FREE_PARAMETER_P[5] * span
                    + FREE_PARAMETER_P[6] * value.f1.f6
                    + FREE_PARAMETER_P[7] * score;

            Tuple10 tuple = new Tuple10<>(
                    value.f0.f0,
                    result,
                    (double)value.f0.f1,
                    (double)value.f1.f1,
                    (double)value.f1.f2,
                    (double)value.f1.f3,
                    (double)value.f1.f4,
                    (double)value.f1.f5,
                    (double)value.f1.f6,
                    (double)value.f1.f7
            );

            return tuple;
        }
    }

    private class finalMapper implements MapFunction<Tuple10<String, Double, Double, Double, Double, Double,
            Double, Double, Double, Double>, Tuple11<String, Double, Double, Double, Double, Double, Double,
            Double, Double, Double, Double>> {
        Double maxValue;
        finalMapper(double maxValue) {
            this.maxValue = maxValue;
        }
        @Override
        public Tuple11<String, Double, Double, Double, Double, Double, Double, Double, Double, Double, Double>
        map(Tuple10<String, Double, Double, Double, Double, Double, Double, Double, Double, Double> value) throws Exception {
            return new Tuple11<>(
                    value.f0,
                    maxValue,
                    value.f1,
                    value.f2,
                    value.f3,
                    value.f4,
                    value.f5,
                    value.f6,
                    value.f7,
                    value.f8,
                    value.f9
            );
        }
    }
}
