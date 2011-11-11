// Copyright (c) 2000 Just Objects B.V. <just@justobjects.nl>
// Distributable under LGPL license. See terms of license at gnu.org.

package nl.justobjects.pushlet.servlet;

import java.io.IOException;
import java.io.InputStreamReader;
import java.util.Enumeration;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import nl.justobjects.pushlet.Version;
import nl.justobjects.pushlet.core.Command;
import nl.justobjects.pushlet.core.Config;
import nl.justobjects.pushlet.core.Dispatcher;
import nl.justobjects.pushlet.core.Event;
import nl.justobjects.pushlet.core.EventParser;
import nl.justobjects.pushlet.core.EventSourceManager;
import nl.justobjects.pushlet.core.Protocol;
import nl.justobjects.pushlet.core.Session;
import nl.justobjects.pushlet.core.SessionManager;
import nl.justobjects.pushlet.util.Log;
import nl.justobjects.pushlet.util.Servlets;

/**
 * Servlet runs a Subscriber per request.
 * 
 * @author Just van den Broecke - Just Objects &copy;
 * @version $Id: Pushlet.java,v 1.23 2007/12/04 13:55:53 justb Exp $
 */
public class Pushlet extends HttpServlet implements Protocol {

  public void init() throws ServletException {
    try {
      // Load configuration (from classpath or WEB-INF root path)
      String webInfPath = getServletContext().getRealPath("/") + "/WEB-INF";
      Config.load(webInfPath);

      Log.init();

      // Start
      Log.info("init() Pushlet Webapp - version=" + Version.SOFTWARE_VERSION + " built=" + Version.BUILD_DATE);

      // Start session manager
      SessionManager.getInstance().start();

      // Start event Dispatcher
      Dispatcher.getInstance().start();

      if (Config.getBoolProperty(Config.SOURCES_ACTIVATE)) {
        EventSourceManager.start(webInfPath);
      } else {
        Log.info("Not starting local event sources");
      }
    } catch (Throwable t) {
      throw new ServletException("Failed to initialize Pushlet framework " + t, t);
    }
  }

  public void destroy() {
    Log.info("destroy(): Exit Pushlet webapp");

    if (Config.getBoolProperty(Config.SOURCES_ACTIVATE)) {
      // Stop local event sources
      EventSourceManager.stop();
    } else {
      Log.info("No local event sources to stop");
    }

    // Should abort all subscribers
    Dispatcher.getInstance().stop();

    // Should stop all sessions
    SessionManager.getInstance().stop();
  }

  /**
   * Servlet GET request: handles event requests.
   */
  public void doGet(HttpServletRequest request, HttpServletResponse response) throws ServletException, IOException {
    Event event = null;

    //填充Event
    try {
      // Event parm identifies event type from the client
      String eventType = Servlets.getParameter(request, P_EVENT);

      // Always must have an event type
      if (eventType == null) {
        Log.warn("Pushlet.doGet(): bad request, no event specified");
        response.sendError(HttpServletResponse.SC_BAD_REQUEST, "No eventType specified");
        return;
      }

      // Create Event and set attributes from parameters
      event = new Event(eventType);
      for (Enumeration e = request.getParameterNames(); e.hasMoreElements();) {
        String nextAttribute = (String) e.nextElement();
        event.setField(nextAttribute, request.getParameter(nextAttribute));
      }

    } catch (Throwable t) {
      // Error creating event
      Log.warn("Pushlets: Error creating event in doGet(): ", t);
      response.setStatus(HttpServletResponse.SC_BAD_REQUEST);
      return;
    }

    // Handle parsed request
    doRequest(event, request, response);

  }

  /**
   * Servlet POST request: extracts event data from body.
   */
  public void doPost(HttpServletRequest request, HttpServletResponse response) throws ServletException, IOException {
    Event event = null;

    //填充Event
    try {
      // Create Event by parsing XML from input stream.
      event = EventParser.parse(new InputStreamReader(request.getInputStream()));

      // Always must have an event type
      if (event.getEventType() == null) {
        Log.warn("Pushlet.doPost(): bad request, no event specified");
        response.sendError(HttpServletResponse.SC_BAD_REQUEST, "No eventType specified");
        return;
      }

    } catch (Throwable t) {
      // Error creating event
      Log.warn("Pushlets:  Error creating event in doPost(): ", t);
      response.setStatus(HttpServletResponse.SC_BAD_REQUEST);
      return;
    }

    // Handle parsed request
    doRequest(event, request, response);

  }

  /**
   * Generic request handler (GET+POST).
   */
  //TODO@ doRequest处理HTTP请求
  protected void doRequest(Event anEvent, HttpServletRequest request, HttpServletResponse response) {
    // Must have valid event type.
    String eventType = anEvent.getEventType();
    try {
      // Get Session: either by creating (on Join eventType)
      // or by id (any other eventType, since client is supposed to have joined).
      Session session = null;
      if (eventType.startsWith(Protocol.E_JOIN)) { //建立会话
        // Join request: create new subscriber
        session = SessionManager.getInstance().createSession(anEvent);

        String userAgent = request.getHeader("User-Agent");
        if (userAgent != null) {
          userAgent = userAgent.toLowerCase();
        } else {
          userAgent = "unknown";
        }
        session.setUserAgent(userAgent);
      } else { //查找服务端是否有此会话
        // Get sessionId
        String id = anEvent.getField(P_ID);

        // We must have an id value
        if (id == null) {
          response.sendError(HttpServletResponse.SC_BAD_REQUEST, "No id specified");
          Log.warn("Pushlets: bad request, no id specified event=" + eventType);
          return;
        }

        // We have an id: get the session object
        session = SessionManager.getInstance().getSession(true, id);

        // Check for invalid id
        if (session == null) {
          response.sendError(HttpServletResponse.SC_BAD_REQUEST, "Invalid or expired id: " + id);
          Log.warn("Pushlets:  bad request, no session found id=" + id + " event=" + eventType);
          return;
        }
      }

      // ASSERTION: we have a valid Session

      // Let Controller handle request further
      // including exceptions
      Command command = Command.create(session, anEvent, request, response); //@wjw_node 封装Command
      session.getController().doCommand(command);
    } catch (Throwable t) {
      // Hmm we should never ever get here
      Log.warn("Pushlets:  Exception in doRequest() event=" + eventType, t);
      t.printStackTrace();
      response.setStatus(HttpServletResponse.SC_INTERNAL_SERVER_ERROR);
    }

  }
}
