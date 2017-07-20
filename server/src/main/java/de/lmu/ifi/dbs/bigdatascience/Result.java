package de.lmu.ifi.dbs.bigdatascience;



/**
 * Created by HuaWentao on 2017/5/31.
 */
import javax.xml.bind.annotation.XmlRootElement;

/** Entity.**/
@XmlRootElement
public class Result {

    /** title from question. **/
    private String title;

    /** redirection to official web page. **/
    private String url;

    /** body from question. **/
    private String preview;


    /**  Get Method.
     * @return get the title**/
    public String getTitle() {
        return title;
    }

    /**
     * Set Method.  *
     * @param title ** title from question
     */
    public void setTitle(String title) {
        this.title = title;
    }

    /**  Get Method.
     * @return get the url**/
    public String getUrl() {
        return url;
    }

    /**
     * Set Method.  *
     * @param url **url to question offical webpage**
     */
    public void setUrl(String url) {
        this.url = url;
    }

    /**  Get Method.
     * @return get the preview content**/
    public String getPreview() {
        return preview;
    }

    /**
     * Set Method.  *
     * @param preview **body from question**
     */
    public void setPreview(String preview) {
        this.preview = preview;
    }
}
