// Copyright (c) 2000 Just Objects B.V. <just@justobjects.nl>
// Distributable under LGPL license. See terms of license at gnu.org.

package nl.justobjects.pushlet.core;

import com.mongodb.BasicDBObject;
import com.mongodb.DBCollection;
import com.mongodb.DBObject;
import com.mongodb.util.JSON;

import nl.justobjects.pushlet.mongodb.MongodbManager;
import nl.justobjects.pushlet.util.Log;
import nl.justobjects.pushlet.util.PushletException;
import nl.justobjects.pushlet.util.Sys;

/**
 * Represents client pushlet session state.
 * 
 * @author Just van den Broecke - Just Objects &copy;
 * @version $Id: Session.java,v 1.8 2007/11/23 14:33:07 justb Exp $
 */
public class Session implements Protocol, ConfigDefs {
  static MongodbManager mongo = MongodbManager.getInstance();
  static final DBCollection _coll = mongo._db.getCollection("session");
  static {
    _coll.createIndex((DBObject) JSON.parse("{'sessionId': 1}"), (DBObject) JSON.parse("{ns: 'pushlet.session', name: 'session_sessionId', unique: true}"));
  }
  private DBObject findPK;

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
    session.findPK = (DBObject) JSON.parse("{'sessionId': '" + session.id + "'}");
    session.controller = Controller.create(session);
    session.subscriber = Subscriber.create(session); //TODO@ 把Subscriber联系上Session

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

  public long getCreateDate() {
    return createDate;
  }

  public boolean isTemporary() {
    return temporary;
  }

  public void setTemporary(boolean temporary) {
    this.temporary = temporary;
    _coll.update(findPK, (DBObject) JSON.parse("{$set: {'temporary': " + temporary + "} }"), false, false);
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
      _coll.update(findPK, (DBObject) JSON.parse("{$set: {'address': '" + address + "'} }"), false, false);
    }
  }

  /**
   * Set event format to encode.
   */
  protected void setFormat(String aFormat) {
    format = aFormat;
    if (aFormat != null) {
      _coll.update(findPK, (DBObject) JSON.parse("{$set: {'format': '" + format + "'} }"), false, false);
    }
  }

  /**
   * Set client HTTP UserAgent.
   */
  public void setUserAgent(String aUserAgent) {
    userAgent = aUserAgent;
    if (aUserAgent != null) {
      _coll.update(findPK, (DBObject) JSON.parse("{$set: {'userAgent': '" + userAgent + "'} }"), false, false);
    }
  }

  /**
   * Decrease time to live.
   */
  public void age(long aDeltaMillis) {
    timeToLive -= aDeltaMillis;
    _coll.update(findPK, (DBObject) JSON.parse("{$set: {'timeToLive': " + timeToLive + "} }"), false, false);
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
    _coll.update(findPK, (DBObject) JSON.parse("{$set: {'timeToLive': " + timeToLive + "} }"), false, false);
  }

  public void start() {
    SessionManager.getInstance().addSession(this);
  }

  public void stop() {
    timeToLive = 0;
    _coll.update(findPK, (DBObject) JSON.parse("{$set: {'timeToLive': " + timeToLive + "} }"), false, false);

    if (this.temporary) {
      _coll.remove(findPK);
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
    return _coll.count(findPK) > 0;
  }

  public void saveStatus() {
    DBObject dbObj = new BasicDBObject();

    dbObj.put("sessionId", id);

    dbObj.put("userAgent", userAgent);
    dbObj.put("createDate", createDate);
    dbObj.put("temporary", temporary);
    dbObj.put("timeToLive", timeToLive);
    dbObj.put("address", address);
    dbObj.put("format", format);

    _coll.findAndModify(findPK, SessionManager.dbObj_ignore_id, null, false, dbObj, true, true);
  }

  public void readStatus() {
    BasicDBObject dbObj = (BasicDBObject) _coll.findOne(findPK);

    id = dbObj.getString("sessionId");

    userAgent = dbObj.getString("userAgent");
    createDate = dbObj.getLong("createDate");
    temporary = dbObj.getBoolean("temporary");
    timeToLive = dbObj.getLong("timeToLive");
    address = dbObj.getString("address");
    format = dbObj.getString("format");
  }

}
