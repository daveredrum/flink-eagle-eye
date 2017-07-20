package de.lmu.ifi.dbs.bigdatascience;

import java.io.File;
import java.nio.file.Files;
import java.util.List;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.operators.Order;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import static org.apache.flink.api.java.aggregation.Aggregations.SUM;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.util.Collector;

import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.StringTokenizer;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
* This class defines methods of getting the indices.
* and writing the indices to .csv files.*/
public class IndexHashmap {
    /**environment value.*/
    private ExecutionEnvironment env;
    /**the number of docs.*/
    private long docCount;
    /**the average length of docs.*/
    private double docAvgLength;
    /**idf output path.*/
    private String idfOutPath;
    /**tf output path.*/
    private String tfOutPath;
    /**data set.*/
    private DataSet<Tuple3<String, String, String>> data;
    /**df.*/
    private DataSet<Tuple3<String, Integer, Integer>> df;
    /**idf.*/
    private DataSet<Tuple2<String, Double>> idf;
    /**tf.*/
    private DataSet<Tuple4<String, String, Integer, Double>> tf;
    /**free parameter of title.*/
    private static double FREE_PARAMETER_1 = 1.0;
    /**free parameter of body.*/
    private static double FREE_PARAMETER_2 = 0.0;

    /**neccessary stop word.*/
    private String[] stopWord;

    /**constructor for initializing the indices.
     * @param env specifies the input execution environment
     * @param inputPath specifies the input path of data set
     * @throws Exception */
    IndexHashmap(final ExecutionEnvironment env, final String inputPath) throws Exception {
        idfOutPath = "../data/output/IDF";
        tfOutPath = "../data/output/TF";
        this.data = getDataSet(env, inputPath);
        this.docCount = this.data.count();
        this.env = env;

        File file = new File("../Stopwords");
        List<String> lines = Files.readAllLines(file.toPath());
        this.stopWord = new String[lines.size()];
        this.stopWord = lines.toArray(this.stopWord);
    }

    /**need to be implemented with actual dataset.
     * @param env specifies the input execution environment
     * @param inputPath specifies the input path of data set
     * @return data set*/
    public DataSet<Tuple3<String, String, String>> getDataSet(final ExecutionEnvironment env, final String inputPath) {
        // read messageId and body fields from the input data set
        return env.readCsvFile(inputPath)
                .lineDelimiter("\n")
                .fieldDelimiter(",")
                .includeFields("111")
                .types(String.class, String.class, String.class);
    }

    /**get the value of df.*/
    private void getDF() {
        df = data
                // extract unique words from docs
                .flatMap(new UniqueWordExtractor(stopWord))
                // compute the frequency of words
                .groupBy(0).sum(1).andSum(2);
    }

    /**compute the Inverse Document Frequency.
    output IDF is a 2-element tuple of <Term, IDF>*/
    private void getIDF() {
        idf = df.flatMap(new IdfComputer(docCount));
    }

    /**compute the Term Frequency.
    output TF is a 3-element tuple of <Term, Document_ID, TF>*/
    private void getTF() {
        tf = data.flatMap(new TFComputer(stopWord))
                .sortPartition(0, Order.ASCENDING);
    }

    /**get the indices.*/
    public void getIndex() {
        this.getDF();
        this.getIDF();
        this.getTF();
    }

    /**write indices to .csv files.
     * @throws Exception */
    public void writeIndex() throws Exception {
        // write IDF to CSV file
        idf.writeAsCsv(idfOutPath, "\n", ",", FileSystem.WriteMode.OVERWRITE);

        // write TF to CSV file
        tf.writeAsCsv(tfOutPath, "\n", ",", FileSystem.WriteMode.OVERWRITE);

        env.execute();

    }

    /**read IDF from existing .csv.
     * @return the IDF.
     * @throws Exception if the input file does not exist.*/
    public DataSet<Tuple2<String, Double>> readIDF() throws Exception {
        return env.readCsvFile(idfOutPath)
                .lineDelimiter("\n")
                .fieldDelimiter(",")
                .types(String.class, Double.class);
    }

    /**read TF from existing .csv files.
     * @return the TF.
     * @throws Exception */
    public DataSet<Tuple4<String, String, Integer, Double>> readTF() throws Exception {
        return env.readCsvFile(tfOutPath)
                .lineDelimiter("\n")
                .fieldDelimiter(",")
                .types(String.class, String.class, Integer.class, Double.class);
    }

    /**get the average length of docs.
     * @return the average length of documents
     * @throws Exception if there are any*/
    public double getDocAvgLength() throws Exception {
        docAvgLength = (double) data.map(new DocMapper())
                .reduce(new DocReducer())
                .collect()
                .get(0) / data.count();
        return docAvgLength;
    }

    /**
     * Extracts the all unique words from the mail body.
     * Words consist only of alphabetical characters. Frequent words (stop words) are filtered out.
     */
    public static class UniqueWordExtractor
            extends RichFlatMapFunction<Tuple3<String, String, String>, Tuple3<String, Integer, Integer>> {
        /** Serial version. */
        public static final long serialVersionUID = 1L;
        /**set of stop words.*/
        private Set<String> stopWords;
        /**set of emitted words.*/
        private Set<String> emittedWords = new HashSet<>();
        /**pattern to match against words.*/
        private Pattern wordPattern = Pattern.compile("(\\p{Alpha})+");

        /**add stop word into stopWords.
         * @param stopWords specifies the new stopWords lists */
        private UniqueWordExtractor(final String[] stopWords) {
            // setup stop words set
            this.stopWords = new HashSet<>();
            Collections.addAll(this.stopWords, stopWords);
        }

        @Override
        public void open(final Configuration config) {
            // init set and word pattern
        }

        @Override
        public void flatMap(final Tuple3<String, String, String> in, final Collector<Tuple3<String, Integer, Integer>> out)
                throws Exception {

            //compute the DF in title
            // clear set of emitted words
            this.emittedWords.clear();
            // split body along whitespaces into tokens
            StringTokenizer st = new StringTokenizer(in.f1);

            // for each word candidate
            while (st.hasMoreTokens()) {
                // normalize to lower case
                String word = st.nextToken().toLowerCase(Locale.US);
                Matcher m = this.wordPattern.matcher(word);
                if (m.matches() && !this.stopWords.contains(word) && !this.emittedWords.contains(word)) {
                    // candidate matches word pattern, is not a stop word, and was not emitted before
                    out.collect(new Tuple3<>(word, 1, 0));
                    this.emittedWords.add(word);
                }
            }

            //compute the DF in body
            // clear set of emitted words
            this.emittedWords.clear();
            // split body along whitespaces into tokens
            st = new StringTokenizer(in.f2);

            // for each word candidate
            while (st.hasMoreTokens()) {
                // normalize to lower case
                String word = st.nextToken().toLowerCase(Locale.US);
                Matcher m = this.wordPattern.matcher(word);
                if (m.matches() && !this.stopWords.contains(word) && !this.emittedWords.contains(word)) {
                    // candidate matches word pattern, is not a stop word, and was not emitted before
                    out.collect(new Tuple3<>(word, 0, 1));
                    this.emittedWords.add(word);
                }
            }
        }
    }

    /**compute the idf.*/
    public static class IdfComputer
            extends RichFlatMapFunction<Tuple3<String, Integer, Integer>, Tuple2<String, Double>> {
        /** Serial version. */
        public static final long serialVersionUID = 1L;
        /**number of emails.*/
        private long number;

        /**get the email amount.
         * @param amount specifies the amount of emails*/
        private IdfComputer(final long amount) {
            number = amount;
        }

        @Override
        public void flatMap(final Tuple3<String, Integer, Integer> docFreq, final Collector<Tuple2<String, Double>> out)
                throws Exception {
            String w = docFreq.f0;
            Double d = Math.log(number / (FREE_PARAMETER_1 * docFreq.f1 + FREE_PARAMETER_2 * docFreq.f2 + 1));
            out.collect(new Tuple2<>(w, d));
        }
    }

    /**
     * Computes the frequency of words in a mails body.
     * Words consist only of alphabetical characters. Frequent words (stop words) are filtered out.
     */
    public static class TFComputer
            extends RichFlatMapFunction<Tuple3<String, String, String>, Tuple4<String, String, Integer, Double>> {

        /** Serial version. */
        public static final long serialVersionUID = 1L;
        /**set of stop words.*/
        private Set<String> stopWords;
        /**map to count the frequency of words.*/
        private Map<String, Tuple2<Integer, Integer>> wordCounts = new HashMap<>();
        /**pattern to match against words.*/
        private Pattern wordPattern = Pattern.compile("(\\p{Alpha})+");

        /**constructor.
         * @param stopWords specifies the stopWords*/
        TFComputer(final String[] stopWords) {
            // initialize stop words
            this.stopWords = new HashSet<>();
            Collections.addAll(this.stopWords, stopWords);
        }

        @Override
        public void open(final Configuration config) {
            // initialized map and pattern
            this.wordCounts.clear();
        }

        @Override
        public void flatMap(final Tuple3<String, String, String> in,
                            final Collector<Tuple4<String, String, Integer, Double>> out)
                throws Exception {

            // clear count map
            this.wordCounts.clear();

            // tokenize mail body along whitespaces
            StringTokenizer st = new StringTokenizer(in.f1);
            // for each candidate word
            int amount_title = 0;
            while (st.hasMoreTokens()) {
                // normalize to lower case
                String word = st.nextToken().toLowerCase(Locale.US);
                Matcher m = this.wordPattern.matcher(word);
                if (m.matches() && !this.stopWords.contains(word)) {
                    // word matches pattern and is not a stop word -> increase word count
                    int count_1 = 0;
                    int count_2 = 0;
                    if (wordCounts.containsKey(word)) {
                        count_1 = wordCounts.get(word).f0;
                        count_2 = wordCounts.get(word).f1;
                    }
                    wordCounts.put(word, new Tuple2<>(count_1 + 1, count_2));
                    amount_title = amount_title + 1;
                }
            }

            // tokenize mail body along whitespaces
            st = new StringTokenizer(in.f2);
            // for each candidate word
            int amount_body = 0;
            while (st.hasMoreTokens()) {
                // normalize to lower case
                String word = st.nextToken().toLowerCase(Locale.US);
                Matcher m = this.wordPattern.matcher(word);
                if (!this.stopWords.contains(word) && m.matches() ) {
                    // word matches pattern and is not a stop word -> increase word count
                    int count_1 = 0;
                    int count_2 = 0;
                    if (wordCounts.containsKey(word)) {
                        count_2 = wordCounts.get(word).f1;
                        count_1 = wordCounts.get(word).f0;
                    }
                    wordCounts.put(word, new Tuple2<>(count_1, count_2 + 1));
                    amount_body = amount_body + 1;
                }
            }

            // emit all counted words
            for (Map.Entry<String, Tuple2<Integer, Integer>> entry : this.wordCounts.entrySet()) {
                double tf = FREE_PARAMETER_1 * (double) entry.getValue().f0 / (double) amount_title
                        + FREE_PARAMETER_2 * (double) entry.getValue().f1 / (double) amount_body;
                out.collect(new Tuple4<>(entry.getKey(), in.f0, in.f1.length(), tf));
            }
        }
    }

    /**map from (DocIndex, DocContent) to (Integer).*/
    public static class DocMapper implements MapFunction<Tuple3<String, String, String>, Integer> {
        /** Serial version. */
        public static final long serialVersionUID = 1L;

        @Override
        public Integer map(Tuple3<String, String, String> value) throws Exception {
            double temp = FREE_PARAMETER_1 * value.f1.length() + FREE_PARAMETER_2 * value.f2.length();
            return (int)temp;
        }
    }

    /**reduce to the total length of docs.*/
    public static class DocReducer implements ReduceFunction<Integer> {
        /** Serial version. */
        public static final long serialVersionUID = 1L;

        @Override
        public Integer reduce(Integer value1, Integer value2) throws Exception {
            return value1 + value2;
        }
    }
}
