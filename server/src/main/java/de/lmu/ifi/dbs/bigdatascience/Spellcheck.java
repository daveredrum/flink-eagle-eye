package de.lmu.ifi.dbs.bigdatascience;

/**
 * Created by HuaWentao on 2017/6/22.
 */


import java.util.Map;
import java.util.HashSet;
import java.util.List;
import java.util.Collections;
import java.util.HashMap;
import java.util.Set;
import java.util.ArrayList;
import java.util.Comparator;


/** correct spell .**/
public class Spellcheck {

    /** 26 characters. **/
    private   final char[] c = {'a', 'b', 'c', 'd', 'e', 'f', 'g', 'h',
            'i', 'j', 'k', 'l', 'm', 'n', 'o', 'p', 'q', 'r', 's', 't', 'u',
            'v', 'w', 'x', 'y', 'z'};
    /** dictionary. **/
    private    Map<String, Integer> trainMap = getmap();



    /**
     * correct spelling.
     * @param  word  word list*
     * @return the right word*
     */
    public   String correct(String word) {
        System.out.print("size:  " + trainMap.size() + "\n");
        Set<String> set = new HashSet<String>();
        String str = known(word, trainMap);
        System.out.print("\nstr length: " + str.length() + "\n");
        if (!"".equals(str)) {
            System.out.print("\nstr null \n");
            return str;
        } else {
            System.out.print("\nstr not null \n");
            set.add(word);
        }
        set.addAll(known(editDistance1(word), trainMap));
        set.addAll(editDistance2(word, trainMap));
        Map<String, Integer> wordsMap = new HashMap<String, Integer>();
        for (String s: set) {
            if (trainMap.get(s) == null) {
                wordsMap.put(s, 0);
            } else {
                wordsMap.put(s, trainMap.get(s));
            }
        }
        List<Map.Entry<String, Integer>> info = new ArrayList<Map.Entry<String, Integer>>(wordsMap.entrySet());
        Collections.sort(info, new Comparator<Map.Entry<String, Integer>>() {
            public int compare(Map.Entry<String, Integer> obj1, Map.Entry<String, Integer> obj2) {
                return obj2.getValue() - obj1.getValue();
            }
        });
        if (info.get(0).getValue() > 0) {
            System.out.print("\nexist   " + info.size() + "\n");
            return info.get(0).getKey();
        } else {
            System.out.print("\nnot exist   " + info.size() + "\n");
            return word;
        }

    }

    /**
     * read dictionary.
     * @return the dictionary*
     */
    private Map<String, Integer> getmap() {
        Dictionary dic = new Dictionary();
        return dic.readdic();
    }



    /**
     *
     *  @param word checked word*
     *  @param trainMap dictionary*
     * @return Set<String>*
     */
    public   Set<String> editDistance2(String word, Map<String, Integer> trainMap) {
        Set<String> editDistance2Set = new HashSet<String>();
        Set<String> tmpSet = new HashSet<String>();
        Set<String> editDistance1Set = editDistance1(word);
        for (String s: editDistance1Set) {
            editDistance2Set.addAll(editDistance1(s));
        }
        for (String s : editDistance2Set) {
            if (!trainMap.containsKey(s)) {
                tmpSet.add(s);
            }
        }
        return tmpSet;
    }

    /**
     * check exist or not.
     * @param wordsSet wordset*
     * @param map dictionary*
     * @return Set<String>*
     */
    public   Set<String> known(Set<String> wordsSet, Map<String, Integer> map) {
        Set<String> set = new HashSet<String>();
        for (String s : wordsSet) {
            if (map.containsKey(s)) {
                set.add(s);
            }
        }
        return set;
    }

    /**
     * check exist or not.
     * @param word checked word*
     * @param map dictionary*
     * @return word or null*
     */
    public   String known(String word, Map<String, Integer> map) {
        if (map.containsKey(word)) {
            return word;
        } else {
            return "";
        }
    }

    /**
     * calculate distance.
     * @param  word  checked word*
     * @return Set<String>*
     */
    public   Set<String> editDistance1(String word) {
        String tempWord = "";
        Set<String> set = new HashSet<String>();
        int n = word.length();
        for (int i = 0; i < n; i++) {
            tempWord = word.substring(0, i) + word.substring(i + 1);
            set.add(tempWord);
        }
        //transposition
        for (int i = 0; i < n - 1; i++) {
            tempWord = word.substring(0, i) + word.charAt(i + 1) + word.charAt(i) + word.substring(i + 2, n);
            set.add(tempWord);
        }

        // alteration 26n
        final int num = 26;
        for (int i = 0; i < n; i++) {
            for (int j = 0; j < num; j++) {
                tempWord = word.substring(0, i) + c[j] + word.substring(i + 1, n);
                set.add(tempWord);
            }
        }

        // insertion 26n
        for (int i = 0; i < n + 1; i++) {
            for (int j = 0; j < num; j++) {
                tempWord = word.substring(0, i) + c[j] + word.substring(i, n);
                set.add(tempWord);
            }
        }
        for (int j = 0; j < num; j++) {
            set.add(word + c[j]);
        }
        return set;
    }
}
