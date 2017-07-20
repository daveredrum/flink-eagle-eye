package de.lmu.ifi.dbs.bigdatascience;



/**
 * Created by HuaWentao on 2017/5/31.
 */
import javax.xml.bind.annotation.XmlRootElement;

/** Entity.**/
@XmlRootElement
public class Suggestion {

    /** title from question. **/
    private String content;




    /**  Get Method.
     * @return get the title**/
    public String getContent() {
        return content;
    }

    /**
     * Set Method.  *
     * @param content ** title from question
     */
    public void setContent(String content) {
        this.content = content;
    }




}
