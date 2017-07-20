package de.lmu.ifi.dbs.bigdatascience;

import java.io.*;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;

/**
 * Created by HuaWentao on 2017/6/25.
 */

/** get dictionary.**/
public class Dictionary {




    /**
     * read dictionary.
     * @return the dictionary*
     */
    public Map<String, Integer> readdic() {
        Map<String, Integer> map = new HashMap<String, Integer>();
        DBConnection conn = new DBConnection(0);
        PreparedStatement sta = conn.getstatement("select * from dic");
        try {
            ResultSet res = sta.executeQuery();
            final int three = 3;
            while (res.next()) {
                map.put(res.getString(2), res.getInt(three));
            }
        } catch (SQLException e) {
            System.out.println("MySQL Opreation ");
            e.printStackTrace();
        } finally {
            conn.close();
            try {
                sta.close();
            } catch (SQLException e) {
                e.printStackTrace();
            }

        }


        return map;
    }

    /**
     * with data to train a dictionary.
     * @return the dictionary*
     */
    public  Map<String, Integer> train() {
        System.out.print("train begin\n");
        File dictionary = new File("../data/input/questions.csv");  // CSV文件路径
        if (dictionary == null) {
            throw new RuntimeException("big.txt not found!!!");
        }
        Map<String, Integer> map = new HashMap<String, Integer>();
        try {
            //读取语料库question.txt
            FileReader fileReader = new FileReader(dictionary);
            final  int num = 512;
            BufferedReader br = new BufferedReader(fileReader, num);
            String s = "";
            while ((s = br.readLine()) != null) {
                s = s.replaceAll("\\pP|\\pS|\\pM|\\pN|\\pC", "");
                s = s.toLowerCase();
                String[] splits = s.split(" ");
                for (int j = 0; j < splits.length; j++) {
                    if (!splits[j].equals(" ")  && !splits[j].equals("") 	&&  splits[j] != null) {
                        if (map.containsKey(splits[j])) {
                            Integer count = map.get(splits[j]);
                            map.put(splits[j], count + 1);
                        } else {
                            map.put(splits[j], 1);
                        }
                    }
                }
            }
            br.close();
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }

        System.out.print("train finish\n");
        return map;
    }


}
