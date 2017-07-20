package de.lmu.ifi.dbs.bigdatascience;

/**
 * Created by HuaWentao on 2017/5/31.
 */

import com.sun.jersey.api.core.PackagesResourceConfig;
import com.sun.jersey.spi.container.servlet.ServletContainer;
import org.mortbay.jetty.Server;
import org.mortbay.jetty.servlet.Context;
import org.mortbay.jetty.servlet.ServletHolder;

/** class for server. **/
public class StartEntity {


    /**start  server main.  *
     * @param args **from virtual machine**
     */
    public static void main(String[] args) {
       startserver();
    }

    /**start  server.  *
     *
     */
    public static void startserver() {
        final int port = 5000;
        Server server = new Server(port);
        ServletHolder sh = new ServletHolder(ServletContainer.class);
        sh.setInitParameter("com.sun.jersey.config.property.resourceConfigClass",
                PackagesResourceConfig.class.getCanonicalName());
        sh.setInitParameter("com.sun.jersey.config.property.packages",
                "de.lmu.ifi.dbs.bigdatascience");

        Context context = new Context(server, null);
        context.addServlet(sh, "/*");
        try {
            server.start();
            server.join();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}