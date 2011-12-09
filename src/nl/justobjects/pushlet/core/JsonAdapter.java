package nl.justobjects.pushlet.core;

import java.io.IOException;
import java.io.PrintWriter;

import javax.servlet.http.HttpServletResponse;

import nl.justobjects.pushlet.util.Log;

/**
 * ClientAdapter that sends Events as Json.
 * 
 * @author wjw
 * 
 */
public class JsonAdapter implements ClientAdapter {
  /**
   * Header for json
   */
  private String contentType = "text/html; charset=UTF-8";
  private PrintWriter out = null;

  private HttpServletResponse servletRsp;
  private String callback = null;
  private int pushCount = 0;

  /**
   * Initialize.
   */
  public JsonAdapter(HttpServletResponse aServletResponse) {
    this(aServletResponse, null);
  }

  /**
   * Initialize.
   */
  public JsonAdapter(HttpServletResponse aServletResponse, String callback) {
    servletRsp = aServletResponse;

    if (callback != null && callback.length() > 0) {
      this.callback = callback;
      contentType = "text/javascript";
    }
  }

  @Override
  public void start() throws IOException {
    servletRsp.setContentType(contentType);

    //@wjw_comment out = servletRsp.getOutputStream();
    out = servletRsp.getWriter(); //@wjw_add 为了正确编码,用writer替换 OutputStream

    // Don't need this further
    servletRsp = null;

    // Start json document if jsonp mode
    if (callback != null) {
      out.print(callback + "(");
    }
    out.print("[");
  }

  @Override
  public void push(Event anEvent) throws IOException {
    debug("event=" + anEvent);

    pushCount++;
    if (pushCount > 1) {
      out.print(",");
    }

    // Send the event as Json to the client and flush.
    out.print(anEvent.toJson());

    if (out.checkError()) { //@wjw_add 把out.flush();改成 out.checkError(),才能判断客户端是否断掉
      throw new IOException("client is broke:" + out);
    }
  }

  /**
   * No action.
   */
  @Override
  public void stop() throws IOException {
    out.print("]");
    if (callback != null) {
      out.print(");");
    }
    out.flush();
  }

  private void debug(String s) {
    Log.debug("[JsonAdapter]" + s);
  }

}
