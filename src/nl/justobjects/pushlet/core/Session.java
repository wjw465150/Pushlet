// Copyright (c) 2000 Just Objects B.V. <just@justobjects.nl>
// Distributable under LGPL license. See terms of license at gnu.org.

package nl.justobjects.pushlet.core;

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
  public static final String REDIS_SESSION_PREFIX = "pushlet:session:";
  private String myHkey;

  private Controller controller;
  private Subscriber subscriber;

  private String userAgent;
  private long LEASE_TIME_MILLIS = Config.getLongProperty(SESSION_TIMEOUT_MINS) * 60 * 1000;
  private volatile long timeToLive = LEASE_TIME_MILLIS;

  public static String[] FORCED_PULL_AGENTS = Config.getProperty(LISTEN_FORCE_PULL_AGENTS).split(",");

  private String address = "unknown";
  private String format = FORMAT_XML;

  private String id;

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

    session.myHkey = REDIS_SESSION_PREFIX + session.id;
    if (session.isPersistence()) {
      session.readStatus();
    }
    session.saveStatus();

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
    //TODO@ wjw考虑在社交环境中,pushlet会话停止而要保留此会话的Subscriber,Controll,EventQueue等所有数据?
    redis.del(myHkey);

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

  public String toString() {
    return getAddress() + "[" + getId() + "]";
  }

  public boolean isPersistence() {
    return redis.exists(myHkey);
  }

  public void saveStatus() {
    if (userAgent != null) {
      redis.hset(myHkey, "userAgent", userAgent);
    }
    redis.hset(myHkey, "timeToLive", String.valueOf(timeToLive));
    if (address != null) {
      redis.hset(myHkey, "address", address);
    }
    if (format != null) {
      redis.hset(myHkey, "format", format);
    }
  }

  public void readStatus() {
    userAgent = redis.hget(myHkey, "userAgent");
    timeToLive = Long.parseLong(redis.hget(myHkey, "timeToLive"));
    address = redis.hget(myHkey, "address");
    format = redis.hget(myHkey, "format");
  }

}
