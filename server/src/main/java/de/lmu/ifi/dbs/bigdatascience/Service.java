package de.lmu.ifi.dbs.bigdatascience;

/**
 * Created by HuaWentao on 2017/5/31.
 */

import java.sql.PreparedStatement;
import java.util.HashMap;
import java.util.List;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.core.MediaType;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Date;
import java.text.SimpleDateFormat;



/**set root path. **/
@Path("/service")
public class Service {



    static String lastquery;

    /**set GET.
     * return results to wegside.
     * * query is the search query from webside*
     * @param  query  **search query from webside**
     * @throws Exception if has error
     * @return the list of results
     */
    @GET
    @Path("/search")   // http://localhost:5000/service/search?query=
    @Produces(MediaType.APPLICATION_JSON)
    public ArrayList<Result> results(@QueryParam("query") String query) throws Exception {

        ArrayList<Result> list = new ArrayList<Result>();
        HashMap<String, Result> map = new HashMap<String, Result>();

        BMScore bm25 = new BMScore(query);
        List<String> ranking = bm25.computeScore();

        if (ranking.size() == 0) {
            return list;
        }

        final int num = 20;
        int  amount = num;

        if (ranking.size() < amount) {
            amount = ranking.size();
        }

        DBConnection conn = new DBConnection(amount);
        PreparedStatement sta = conn.getstatement("select * from question where Id = ?");


        System.out.print("size" + amount);
        for (int i = 1; i < amount + 1; i++) {
            sta.setInt(i, Integer.parseInt(ranking.get(i - 1)));
        }

        try {
            ResultSet res = sta.executeQuery();
            final int three = 3;
            while (res.next()) {
                Result r = new Result();

                r.setPreview(res.getString(three));
                r.setTitle(res.getString(2));
                r.setUrl(res.getString(three + 2));
                map.put(res.getString(1), r);
            }
        } catch (SQLException e) {
            System.out.println("MySQL Opreation ");
            e.printStackTrace();
        }



        for (int i = 0; i < amount; i++) {
            list.add(map.get(ranking.get(i)));
        }


        /*for (int i = 1; i < amount + 1; i++) {
            sta.setInt(1, Integer.parseInt(ranking.get(i - 1)));
            try {
                ResultSet res = sta.executeQuery();
                final int three = 3;
                if (res.next()) {
                    Result r = new Result();

                    r.setPreview(res.getString(three));
                    r.setTitle(res.getString(2));
                    r.setUrl(res.getString(three + 2));
                    list.add(r);
                }
            } catch (SQLException e) {
                System.out.println("MySQL Opreation ");
                e.printStackTrace();
            }

        }*/



        sta.close();
        conn.close();


        SimpleDateFormat df = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");

        System.out.println(df.format(new Date()) + "search       Query: " + query + "\n");
        return list;
    }


    /**set GET.
     * return suggestions to wegside.
     * * query is the search query from webside*
     * @param  query  **search query from webside**
     * @throws Exception if has error
     * @return the list of suggestions
     */
    @GET
    @Path("/suggestion")   // http://localhost:5000/service/suggestion?query=
    @Produces(MediaType.APPLICATION_JSON)
    public ArrayList<Suggestion> suggestion(@QueryParam("query") String query)  {
        System.out.print("Begin suggestion!\n");
        lastquery = query;

        Spellcheck spellc = new Spellcheck();
        ResultSet resultset;
        ArrayList<ArrayList<String>> list = new ArrayList<>();
        ArrayList<String> wordlist = removenullword(query);
        if (query.contains(" ")) {     //contains space
            //System.out.print("\nin1\n");
            if (query.substring(query.length() - 1, query.length()).equals(" ")) {   //last character is space
                //System.out.print("\nin2\n");
                wordlist = spellcheck(wordlist, wordlist.size(), spellc);
                if (wordlist.size() == 1) {   //    type  1
                    String sql = "SELECT goalword FROM bgram WHERE firstword = ?  order by amount desc";
                    DBConnection conn = new DBConnection(0);
                    PreparedStatement sta = conn.getstatement(sql);
                    try {
                        sta.setString(1, wordlist.get(0));
                        resultset = sta.executeQuery();
                        list = addgoalword(wordlist, "1", resultset);
                    } catch (SQLException e) {
                        e.printStackTrace();
                    } finally {
                        conn.close();
                        try {
                            sta.close();
                        } catch (SQLException e) {
                            e.printStackTrace();
                        }
                    }

                }
                if (wordlist.size() > 1) {    //    type  1
                    //System.out.print("进来了\n");
                    String sql = "SELECT goalword FROM tgram WHERE firstword = ? "
                                                 + "and secondword = ? order by amount desc";
                    DBConnection conn = new DBConnection(0);
                    PreparedStatement sta = conn.getstatement(sql);
                    try {
                        sta.setString(1, wordlist.get(wordlist.size() - 2));
                        //System.out.print(wordlist.get(wordlist.size() - 2)+"\n\n\n\n\n");
                        sta.setString(2, wordlist.get(wordlist.size() - 1));
                        //System.out.print(wordlist.get(wordlist.size() - 1)+"\n\n\n\n\n");
                        resultset = sta.executeQuery();
                        list = addgoalword(wordlist, "1", resultset);
                        //System.out.print("大小："+list.size()+resultset);
                    } catch (SQLException e) {
                        e.printStackTrace();
                    } finally {
                        conn.close();
                        try {
                            sta.close();
                        } catch (SQLException e) {
                            e.printStackTrace();
                        }
                    }

                }

            } else {     //last character is not space

                wordlist = spellcheck(wordlist, wordlist.size() - 1, spellc);
                if (wordlist.size() > 2) {   //type   2
                    String sql = "SELECT goalword FROM tgram WHERE firstword = ? "
                                    + "and secondword = ? and goalword like ? order by amount desc";
                    DBConnection conn = new DBConnection(0);
                    PreparedStatement sta = conn.getstatement(sql);
                    final int three = 3;
                    try {
                        sta.setString(1, wordlist.get(wordlist.size() - three));
                        sta.setString(2, wordlist.get(wordlist.size() - 2));
                        sta.setString(three, wordlist.get(wordlist.size() - 1) + "%");
                        resultset = sta.executeQuery();
                        list = addgoalword(wordlist, "2", resultset);
                    } catch (SQLException e) {
                        e.printStackTrace();
                    } finally {
                        conn.close();
                        try {
                            sta.close();
                        } catch (SQLException e) {
                            e.printStackTrace();
                        }
                    }

                } else {   //type   2
                    String sql = "SELECT goalword FROM bgram WHERE firstword = ? "
                                        + "and goalword like ? order by amount desc";
                    DBConnection conn = new DBConnection(0);
                    PreparedStatement sta = conn.getstatement(sql);
                    try {
                        sta.setString(1, wordlist.get(wordlist.size() - 2));
                        sta.setString(2, wordlist.get(wordlist.size() - 1) + "%");
                        resultset = sta.executeQuery();
                        list = addgoalword(wordlist, "2", resultset);
                    } catch (SQLException e) {
                        e.printStackTrace();
                    } finally {
                        conn.close();
                        try {
                            sta.close();
                        } catch (SQLException e) {
                            e.printStackTrace();
                        }
                    }

                }

            }
        } else {    //no space
            String sql = "SELECT goalword FROM ugram WHERE goalword LIKE  ?  order by amount desc";

            DBConnection conn = new DBConnection(0);
            PreparedStatement sta = conn.getstatement(sql);

            try {
                sta.setString(1, query + "%");
                resultset = sta.executeQuery();
                list = addgoalword(wordlist, "2", resultset);
            } catch (SQLException e) {
                e.printStackTrace();
            } finally {
                conn.close();
                try {
                    sta.close();
                } catch (SQLException e) {
                    e.printStackTrace();
                }
            }
        }

        ArrayList<Suggestion> fresult = new ArrayList<>();
        ArrayList<Suggestion> emptyresult = new ArrayList<>();

        if ( !query.equals(lastquery)) {
            System.out.print("thread kill\n");
            return emptyresult;
        }
        for (ArrayList<String>  q : list) {
            ArrayList<String> tempsuggestion = getgoalword(q, q.size());
            StringBuffer tempquery = new StringBuffer(tempsuggestion.get(0));

            for (int i = 1; i < tempsuggestion.size(); i++) {
                tempquery.append(" ").append(tempsuggestion.get(i));
            }
            Suggestion su = new Suggestion();
            su.setContent(tempquery.toString());
            System.out.print("show:" + su.getContent() + "\n");
            fresult.add(su);
            if ( !query.equals(lastquery)) {
                System.out.print("thread kill\n");
                return emptyresult;
            }
        }

        if ( !query.equals(lastquery)) {
            System.out.print("thread kill\n");
            return emptyresult;
        }
        SimpleDateFormat df = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");

        System.out.println(df.format(new Date()) + "     Suggestion       Query: " + query + "\n");
        return fresult;

    }


    /**
     * with words to find goalword.
     * @param  wordlist  original word list*
     * @param  olength  original length*
     * @throws SQLException if has error*
     * @return the list after add goal words*
     */
    public  ArrayList<String> getgoalword(ArrayList<String> wordlist, int olength)  {

        final int threshold = 8;
        if (wordlist.size() == 1) {   //    type  1
            String sql = "SELECT goalword,amount FROM bgram WHERE firstword = ?  order by amount desc";
            DBConnection conn = new DBConnection(0);
            PreparedStatement sta = conn.getstatement(sql);
            try {
                sta.setString(1, wordlist.get(0));
                ResultSet resultset = sta.executeQuery();
                if (resultset.next()) {
                    final  int three = 5;
                    if (resultset.getInt(2) < threshold || wordlist.size() - olength > three) {
                        return wordlist;
                    } else {
                        wordlist.add(resultset.getString(1));
                        getgoalword(wordlist, olength);
                    }
                } else {
                    return wordlist;
                }
            } catch (SQLException e) {
                e.printStackTrace();
            } finally {
                conn.close();
                try {
                    sta.close();
                } catch (SQLException e) {
                    e.printStackTrace();
                }
            }


        }
        if (wordlist.size() > 1) {    //    type  1
            String sql = "SELECT goalword,amount FROM tgram WHERE firstword = ? "
                         + "and secondword = ? order by amount desc";
            DBConnection conn = new DBConnection(0);
            PreparedStatement sta = conn.getstatement(sql);
            try {
                sta.setString(1, wordlist.get(wordlist.size() - 1));
                //System.out.print(wordlist.get(wordlist.size() - 1)+"\n\n\n\n\n");
                sta.setString(2, wordlist.get(wordlist.size() - 2));
                //System.out.print(wordlist.get(wordlist.size() - 2)+"\n\n\n\n\n");
                ResultSet resultset = sta.executeQuery();
                if (resultset.next()) {
                    final int three = 3;
                    if (resultset.getInt(2) < threshold || wordlist .size() - olength > three) {
                        return wordlist;
                    } else {
                        wordlist.add(resultset.getString(1));
                        getgoalword(wordlist, wordlist.size());
                    }
                } else {
                    return wordlist;
                }
            } catch (SQLException e) {
                e.printStackTrace();
            } finally {
                conn.close();
                try {
                    sta.close();
                } catch (SQLException e) {
                    e.printStackTrace();
                }
            }

        }
        return wordlist;
    }

    /**
     * removeall null word.
     * @param  query  search query*
     * @return the list after add goal words*
     */
    public ArrayList<String> removenullword(String query) {
        String[] word = query.split(" ");
        ArrayList<String> list = new ArrayList<>();
        for (String w : word) {
            if (w != null) {
                list.add(w);
            }
        }
        return list;

    }

    /**
     * add goal word.
     * @param  word  word list*
     * @param  type chose bgram or tgram*
     * @param  result resultset from db*
     * @throws SQLException if ha error*
     * @return the list after add goal words*
     * */
    public ArrayList<ArrayList<String>> addgoalword(
                        ArrayList<String> word, String type, ResultSet result)  {


        ArrayList<ArrayList<String>> list = new ArrayList<>();

        int i = 0;
        if (type.equals("1")) {
            try {
               // System.out.print("1打印了几次\n");
                while (result.next()) {
                    //System.out.print("type 1\n");
                    ArrayList<String> newword = new ArrayList<>(word);
                    list.add(newword);
                    list.get(i).add(result.getString(1));
                    i++;
                    //System.out.print("打印了几次\n");
                    final int five = 5;
                    if (i == five) {
                        break;
                    }
                }
            } catch (SQLException e) {
                e.printStackTrace();
            } finally {
                try {
                    result.close();
                } catch (SQLException e) {
                    e.printStackTrace();
                }

            }
        } else {
            // System.out.print("\nin type 2\n");
            try {
                while (result.next()) {
                    //System.out.print("\ntype 2\n"+"result:"+result.getString(1)+"\n");
                    ArrayList<String> newword = new ArrayList<>(word);

                    list.add(newword);
                    list.get(i).set((word.size() - 1), result.getString(1));
                    // System.out.print("list:"+list.get(i).get(1).toString());
                    i++;
                    final int five = 5;
                    if (i == five) {
                        break;
                    }
                 }
            } catch (SQLException e) {
                e.printStackTrace();
            } finally {
                try {
                    result.close();
                } catch (SQLException e) {
                    e.printStackTrace();
                }

            }
        }
        return list;
    }

    /**
     * return correct word list.
     * @param  list  word list*
     * @param  n check how many words*
     * @param spellc class  spellcheck*
     * @return the list of right word*
     */
    private ArrayList<String> spellcheck(ArrayList<String> list, int n, Spellcheck spellc) {

        for (int i = 0; i < n; i++) {
            list.set(i, spellc.correct(list.get(i)));
        }
        return list;
    }






}
