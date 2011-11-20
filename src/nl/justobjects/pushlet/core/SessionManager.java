// Copyright (c) 2000 Just Objects B.V. <just@justobjects.nl>
// Distributable under LGPL license. See terms of license at gnu.org.

package nl.justobjects.pushlet.core;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.rmi.server.UID;
import java.util.Date;
import java.util.Map;
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.ConcurrentHashMap;

import nl.justobjects.pushlet.redis.RedisManager;
import nl.justobjects.pushlet.util.Log;
import nl.justobjects.pushlet.util.PushletException;
import nl.justobjects.pushlet.util.Rand;
import nl.justobjects.pushlet.util.Sys;

/**
 * Manages lifecycle of Sessions.
 * 
 * @author Just van den Broecke - Just Objects &copy;
 * @version $Id: SessionManager.java,v 1.12 2007/12/04 13:55:53 justb Exp $
 */
public class SessionManager implements ConfigDefs {
  static RedisManager redis = RedisManager.getInstance();

  static final String PUSHLET_ALL_SESSION = "p:a:s";
  static final String PUSHLET_SET_SESSION = "p:s:s";

  /**
   * Singleton pattern: single instance.
   */
  private static SessionManager instance;

  static {
    // Singleton + factory pattern:  create single instance
    // from configured class name
    try {
      instance = (SessionManager) Config.getClass(SESSION_MANAGER_CLASS, "nl.justobjects.pushlet.core.SessionManager").newInstance();
      Log.info("SessionManager created className=" + instance.getClass());
    } catch (Throwable t) {
      Log.fatal("Cannot instantiate SessionManager from config", t);
    }
  }

  /**
   * Timer to schedule session leasing TimerTasks.
   */
  private Timer timer;
  private final long TIMER_INTERVAL_MILLIS = 60000;

  /**
   * Map of active sessions, keyed by their id, all access is through mutex.
   */
  private Map<String, Session> sessions = new ConcurrentHashMap<String, Session>(13);

  /**
   * Singleton pattern: protected constructor needed for derived classes.
   */
  protected SessionManager() {
  }

  /**
   * Visitor pattern implementation for Session iteration.
   * <p/>
   * This method can be used to iterate over all Sessions in a threadsafe way.
   * See Dispatcher.multicast and broadcast methods for examples.
   * 
   * @param visitor
   *          the object that should implement method parm
   * @param method
   *          the method to be called from visitor
   * @param args
   *          arguments to be passed in visit method, args[0] will always be
   *          Session object
   */
  public void apply(Object visitor, Method method, Object[] args) {
    // Valid session cache: loop and call supplied Visitor method
    for (Session nextSession : sessions.values()) {
      // Session cache may not be entirely filled
      if (nextSession == null) {
        break;
      }

      try {
        // First argument is always a Session object
        args[0] = nextSession;

        // Use Java reflection to call the method passed by the Visitor
        method.invoke(visitor, args); //TODO@ see Dispatcher.SessionManagerVisitor#visitMulticast
      } catch (IllegalAccessException e) {
        Log.warn("apply: illegal method access: ", e);
      } catch (InvocationTargetException e) {
        Log.warn("apply: method invoke: ", e);
      }
    }

    //@wjw_add 当服务器发出E_ABORT消息时,只发给当前节点的Session
    if (args.length == 2 && args[1] instanceof Event) {
      Event event = (Event) args[1];
      if (event.getEventType().equals(Protocol.E_ABORT)) {
        return;
      }
    }

    //@wjw_add 在查找本地没有,而redis有的其他节点上的session
    // @wjw_note 方法1->分批获取所有redis里的session
    java.util.List<byte[]> allSessions = null;
    int start = 0;
    String tempSessionId;
    Session tempSession;
    while (true) {
      allSessions = redis.lrange(PUSHLET_ALL_SESSION, start, start + 99);
      if (allSessions.size() == 0) {
        break;
      }

      start = start + allSessions.size();
      for (byte[] bb : allSessions) {
        try {
          tempSessionId = new String(bb, RedisManager.REDIS_CHARSET);
          if (sessions.containsKey(tempSessionId) == false) {
            tempSession = Session.create(tempSessionId);
            tempSession.getSubscriber().start();

            args[0] = tempSession;
            method.invoke(visitor, args); //TODO@ see Dispatcher.SessionManagerVisitor#visitMulticast
          }
        } catch (Exception e) {
          Log.warn("apply: method invoke: ", e);
        }
      }
    }

    // @wjw_note 方法2->此获取所有redis里的session方法,太耗内存!
    //    java.util.Set<byte[]> sessionsSet = redis.keys(Session.PUSHLET_SESSION_PREFIX + "*");
    //    String tempSessionId;
    //    Session tempSession;
    //    for (byte[] bb : sessionsSet) {
    //      try {
    //        tempSessionId = new String(bb, redis.REDIS_CHARSET);
    //        tempSessionId = tempSessionId.substring(Session.PUSHLET_SESSION_PREFIX.length());
    //        if (sessions.containsKey(tempSessionId) == false) {
    //          tempSession = Session.create(tempSessionId);
    //          tempSession.getSubscriber().start();
    //
    //          args[0] = tempSession;
    //          method.invoke(visitor, args); //TODO@ see Dispatcher.SessionManagerVisitor#visitMulticast
    //        }
    //      } catch (Exception e) {
    //        Log.warn("apply: method invoke: ", e);
    //      }
    //    }

  }

  /**
   * Create new Session (but add later).
   */
  public Session createSession(Event anEvent) throws PushletException {
    if (anEvent.getField(Protocol.P_ID) == null) {
      return Session.create(createSessionId());
    } else {
      Session session = Session.create(anEvent.getField(Protocol.P_ID));
      session.setTemporary(false); //说明不是临时会话
      return session;
    }
  }

  /**
   * Singleton pattern: get single instance.
   */
  public static SessionManager getInstance() {
    return instance;
  }

  /**
   * Get Session by session id.
   */
  public Session getSession(boolean canAdd, String anId) {
    Session tmpSession = (Session) sessions.get(anId);

    //@wjw_add 再从redis里查询是否有此anId的session
    if (tmpSession == null && redis.exists(Session.PUSHLET_SESSION_PREFIX + anId)) {
      try {
        tmpSession = Session.create(anId);
        tmpSession.getSubscriber().start();
        if (canAdd) {
          this.addSession(tmpSession);
        }
      } catch (PushletException e) {
        tmpSession = null;
        Log.warn(e.getMessage());
      }
    }

    return tmpSession;
  }

  /**
   * Get copy of listening Sessions.
   */
  public Session[] getSessions() {
    return (Session[]) sessions.values().toArray(new Session[0]);
  }

  /**
   * Get number of listening Sessions.
   */
  public int getSessionCount() {
    return sessions.size();
  }

  /**
   * Get status info.
   */
  public String getStatus() {
    Session[] sessions = getSessions();
    StringBuffer statusBuffer = new StringBuffer();
    statusBuffer.append("SessionMgr: " + sessions.length + " sessions \\n");
    for (int i = 0; i < sessions.length; i++) {
      statusBuffer.append(sessions[i] + "\\n");
    }
    return statusBuffer.toString();
  }

  /**
   * Is Session present?.
   */
  public boolean hasSession(String anId) {
    return sessions.containsKey(anId);
  }

  /**
   * Add session.
   */
  public void addSession(Session session) {
    sessions.put(session.getId(), session);

    info(session.getId() + " at " + session.getAddress() + " added ");
  }

  /**
   * Register session for removal.
   */
  public Session removeSession(Session aSession) {
    Session session = (Session) sessions.remove(aSession.getId());
    if (session != null) {
      info(session.getId() + " at " + session.getAddress() + " removed ");
    }
    return session;
  }

  /**
   * Starts us.
   */
  public void start() throws PushletException {
    if (timer != null) {
      stop();
    }
    timer = new Timer(false);
    timer.schedule(new AgingTimerTask(), TIMER_INTERVAL_MILLIS, TIMER_INTERVAL_MILLIS);
    info("started; interval=" + TIMER_INTERVAL_MILLIS + "ms");
  }

  /**
   * Stopis us.
   */
  public void stop() {
    if (timer != null) {
      timer.cancel();
      timer = null;
    }

    //->@wjw_add 原作者在停止SessionManager时没有销毁session,在使用redis持久化时会残留垃圾信息.
    Session[] arraySession = getSessions();
    for (Session ss : arraySession) {
      ss.stop();
    }
    //<-@wjw_add

    sessions.clear();
    info("stopped");
  }

  /**
   * Create unique Session id.
   */
  protected String createSessionId() {
    // Use UUID if specified in config (thanks Uli Romahn)
    if (Config.hasProperty(SESSION_ID_GENERATION)
        && Config.getProperty(SESSION_ID_GENERATION).equals(SESSION_ID_GENERATION_UUID)) {
      // We want to be Java 1.4 compatible so use UID class (1.5+ we may use java.util.UUID). 
      return new UID().toString();
    }

    // Other cases use random name

    String id;
    while (true) {
      id = Rand.randomName(Config.getIntProperty(SESSION_ID_SIZE));
      if (!hasSession(id)) {
        // Created unique session id
        break;
      }
    }
    return id;
  }

  /**
   * Util: stdout printing.
   */
  protected void info(String s) {
    Log.info("SessionManager: " + new Date() + " " + s);
  }

  /**
   * Util: stdout printing.
   */
  protected void warn(String s) {
    Log.warn("SessionManager: " + s);
  }

  /**
   * Util: stdout printing.
   */
  protected void debug(String s) {
    Log.debug("SessionManager: " + s);
  }

  /**
   * Manages Session timeouts.
   */
  private class AgingTimerTask extends TimerTask {
    private long lastRun = Sys.now();
    private long delta;
    private Method visitMethod;

    public AgingTimerTask() throws PushletException {
      try {
        // Setup Visitor Methods for callback from SessionManager
        Class[] argsClasses = { Session.class };
        visitMethod = this.getClass().getMethod("visit", argsClasses);
      } catch (NoSuchMethodException e) {
        throw new PushletException("Failed to setup AgingTimerTask", e);
      }
    }

    /**
     * Clock tick callback from Timer.
     */
    public void run() {
      long now = Sys.now();
      delta = now - lastRun;
      lastRun = now;
      debug("AgingTimerTask: tick");

      // Use Visitor pattern to loop through Session objects (see visit() below)
      getInstance().apply(this, visitMethod, new Object[1]);
    }

    /**
     * Callback from SessionManager during apply()
     */
    public void visit(Session aSession) {
      try {
        // Age the lease
        aSession.age(delta);
        debug("AgingTimerTask: visit: " + aSession);

        // Stop session if lease expired
        if (aSession.isExpired()) {
          if (getInstance().hasSession(aSession.getId())) {
            info("AgingTimerTask: Session expired: " + aSession);
          }

          aSession.stop();
        }
      } catch (Throwable t) {
        warn("AgingTimerTask: Error in timer task : " + t);
      }
    }
  }
}
