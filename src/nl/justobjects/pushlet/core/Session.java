// Copyright (c) 2000 Just Objects B.V. <just@justobjects.nl>
// Distributable under LGPL license. See terms of license at gnu.org.

package nl.justobjects.pushlet.core;

import java.util.HashMap;
import java.util.Map;

import nl.justobjects.pushlet.redis.RedisManager;
import nl.justobjects.pushlet.util.Log;
import nl.justobjects.pushlet.util.PushletException;

/**
 * Represents client pushlet session state.
 * 
 * @author Just van den Broecke - Just Objects &copy;
 * @version $Id: Session.java,v 1.8 2007/11/23 14:33:07 justb Exp $
 */
public class Session implements Protocol, ConfigDefs {
  static RedisManager redis = RedisManager.getInstance();
  static final String PUSHLET_SESSION_PREFIX = "p:ss:";
  private String myHkey;

  private Controller controller;
  private Subscriber subscriber;

  private String userAgent;
  private long LEASE_TIME_MILLIS = Config.getLongProperty(SESSION_TIMEOUT_MINS) * 60 * 1000;

  public static String[] FORCED_PULL_AGENTS = Config.getProperty(LISTEN_FORCE_PULL_AGENTS).split(",");

  private String address = "unknown";
  private String format = FORMAT_XML;

  private String id;
  private volatile long timeToLive = LEASE_TIME_MILLIS; //@wjw_node session会话的timeout时间
  //@wjw_node session的创建时间,在用户join时用当前系统时间更新,
  private long createDate = System.currentTimeMillis();
  //@wjw_node SessionManager.AgingTimerTask里根据temporary属性的值来决定是否清除悬挂的session.
  private boolean temporary = true; //@wjw_node 标志session是否是临时的

  /**
   * Protected constructor as we create through factory method.
   */
  protected Session() {
  }

  /**
   * Create instance through factory method.
   * 
   * @param anId
   *          a session id
   * @return a Session object (or derived)
   * @throws PushletException
   *           exception, usually misconfiguration
   */
  public static Session create(String anId) throws PushletException {
    Session session;
    try {
      session = (Session) Config.getClass(SESSION_CLASS, "nl.justobjects.pushlet.core.Session").newInstance();
    } catch (Throwable t) {
      throw new PushletException("Cannot instantiate Session from config", t);
    }

    // Init session
    session.id = anId;
    session.controller = Controller.create(session);
    session.subscriber = Subscriber.create(session); //TODO@ 把Subscriber联系上Session

    session.myHkey = PUSHLET_SESSION_PREFIX + session.id;
    if (session.isPersistence()) {
      session.readStatus();
    } else {
      session.saveStatus();
    }

    return session;
  }

  /**
   * Return (remote) Subscriber client's IP address.
   */
  public String getAddress() {
    return address;
  }

  /**
   * Return command controller.
   */
  public Controller getController() {
    return controller;
  }

  /**
   * Return Event format to send to client.
   */
  public String getFormat() {
    return format;
  }

  /**
   * Return (remote) Subscriber client's unique id.
   */
  public String getId() {
    return id;
  }

  public long getCreateDate() {
    return createDate;
  }

  public boolean isTemporary() {
    return temporary;
  }

  public void setTemporary(boolean temporary) {
    this.temporary = temporary;
    redis.hset(myHkey, "temporary", String.valueOf(temporary));
  }

  /**
   * Return subscriber.
   */
  public Subscriber getSubscriber() {
    return subscriber;
  }

  /**
   * Return remote HTTP User-Agent.
   */
  public String getUserAgent() {
    return userAgent;
  }

  /**
   * Set address.
   */
  protected void setAddress(String anAddress) {
    address = anAddress;
    if (anAddress != null) {
      redis.hset(myHkey, "address", address);
    }
  }

  /**
   * Set event format to encode.
   */
  protected void setFormat(String aFormat) {
    format = aFormat;
    if (aFormat != null) {
      redis.hset(myHkey, "format", format);
    }
  }

  /**
   * Set client HTTP UserAgent.
   */
  public void setUserAgent(String aUserAgent) {
    userAgent = aUserAgent;
    if (aUserAgent != null) {
      redis.hset(myHkey, "userAgent", userAgent);
    }
  }

  /**
   * Decrease time to live.
   */
  public void age(long aDeltaMillis) {
    timeToLive -= aDeltaMillis;
    redis.hset(myHkey, "timeToLive", String.valueOf(timeToLive));
  }

  /**
   * Has session timed out?
   */
  public boolean isExpired() {
    return timeToLive <= 0;
  }

  /**
   * Keep alive by resetting TTL.
   */
  public void kick() {
    timeToLive = LEASE_TIME_MILLIS;
    redis.hset(myHkey, "timeToLive", String.valueOf(timeToLive));
  }

  public void start() {
    SessionManager.getInstance().addSession(this);
  }

  public void stop() {
    timeToLive = 0;
    redis.hset(myHkey, "timeToLive", String.valueOf(timeToLive));

    if (this.temporary) {
      //->先把ID从键为("p:zset:as"的ZSet)里删除
      if (redis.zscore(SessionManager.PUSHLET_ZSET_ALLSESSION, id) != null) {
        redis.zrem(SessionManager.PUSHLET_ZSET_ALLSESSION, id);
      }
      //<-先把ID从键为("p:zet:as"的ZSet)里删除

      redis.del(myHkey);
    }

    subscriber.stop();
    SessionManager.getInstance().removeSession(this);
  }

  /**
   * Info.
   */
  public void info(String s) {
    Log.info("S-" + this + ": " + s);
  }

  /**
   * Exceptional print util.
   */
  public void warn(String s) {
    Log.warn("S-" + this + ": " + s);
  }

  /**
   * Exceptional print util.
   */
  public void debug(String s) {
    Log.debug("S-" + this + ": " + s);
  }

  @Override
  public String toString() {
    return "Session [timeToLive=" + timeToLive + ", address=" + address + ", format=" + format + ", id=" + id
        + ", createDate=" + createDate + ", temporary=" + temporary + "]";
  }

  public boolean isPersistence() {
    return redis.exists(myHkey);
  }

  public void saveStatus() {
    Map<String, String> keyValues = new HashMap<String, String>(8);

    if (userAgent != null) {
      keyValues.put("userAgent", userAgent);
    }
    keyValues.put("createDate", String.valueOf(createDate));
    keyValues.put("temporary", String.valueOf(temporary));
    keyValues.put("timeToLive", String.valueOf(timeToLive));
    if (address != null) {
      keyValues.put("address", address);
    }
    if (format != null) {
      keyValues.put("format", format);
    }

    redis.hmset(myHkey, keyValues);

    //->再把ID放到键为("p:zset:as"的ZSet)里
    if (redis.zscore(SessionManager.PUSHLET_ZSET_ALLSESSION, id) == null) {
      redis.zadd(SessionManager.PUSHLET_ZSET_ALLSESSION, System.currentTimeMillis(), id);
    }
    //<-再把ID放到键为("p:zset:as"的ZSet)里

    keyValues.clear();
  }

  public void readStatus() {
    java.util.Map<String, String> keyValues = redis.hgetAll(myHkey);

    String tmpStr;
    userAgent = keyValues.get("userAgent");
    tmpStr = keyValues.get("createDate");
    if (tmpStr != null) {
      createDate = Long.parseLong(tmpStr);
    }
    tmpStr = keyValues.get("temporary");
    if (tmpStr != null) {
      temporary = Boolean.parseBoolean(tmpStr);
    }
    tmpStr = keyValues.get("timeToLive");
    if (tmpStr != null) {
      timeToLive = Long.parseLong(tmpStr);
    }
    address = keyValues.get("address");
    format = keyValues.get("format");

    keyValues.clear();
  }

}
