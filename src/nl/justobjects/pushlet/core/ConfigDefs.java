// Copyright (c) 2000 Just Objects B.V. <just@justobjects.nl>
// Distributable under LGPL license. See terms of license at gnu.org.

package nl.justobjects.pushlet.core;

/**
 * Definition of config property strings.
 * 
 * @author Just van den Broecke - Just Objects &copy;
 * @version $Id: ConfigDefs.java,v 1.9 2007/12/07 12:57:40 justb Exp $
 */
public interface ConfigDefs {
  /**
   * Class factory definitions, used to insert your custom classes.
   */
  public static final String CONTROLLER_CLASS = "controller.class";
  public static final String DISPATCHER_CLASS = "dispatcher.class";
  public static final String LOGGER_CLASS = "logger.class";
  public static final String SESSION_MANAGER_CLASS = "sessionmanager.class";
  public static final String SESSION_CLASS = "session.class";
  public static final String SUBSCRIBER_CLASS = "subscriber.class";
  public static final String SUBSCRIPTION_CLASS = "subscription.class";

  /**
   * Session management.
   */
  public static final String SESSION_ID_SIZE = "session.id.size";
  public static final String SESSION_ID_GENERATION = "session.id.generation";
  public static final String SESSION_ID_GENERATION_UUID = "uuid";
  public static final String SESSION_ID_GENERATION_RANDOMSTRING = "randomstring";
  public static final String SESSION_TIMEOUT_MINS = "session.timeout.mins";

  public static final String SOURCES_ACTIVATE = "sources.activate";

  /**
   * Logging
   */
  public static final String LOG_LEVEL = "log.level";
  public static final int LOG_LEVEL_FATAL = 1;
  public static final int LOG_LEVEL_ERROR = 2;
  public static final int LOG_LEVEL_WARN = 3;
  public static final int LOG_LEVEL_INFO = 4;
  public static final int LOG_LEVEL_DEBUG = 5;
  public static final int LOG_LEVEL_TRACE = 6;

  /**
   * Queues
   */
  public static final String QUEUE_SIZE = "queue.size";
  public static final String QUEUE_READ_TIMEOUT_MILLIS = "queue.read.timeout.millis";
  public static final String QUEUE_WRITE_TIMEOUT_MILLIS = "queue.write.timeout.millis";

  /**
   * Listening modes.
   */
  public static final String LISTEN_FORCE_PULL_ALL = "listen.force.pull.all";
  public static final String LISTEN_FORCE_PULL_AGENTS = "listen.force.pull.agents";

  public static final String PULL_REFRESH_TIMEOUT_MILLIS = "pull.refresh.timeout.millis";
  public static final String PULL_REFRESH_WAIT_MIN_MILLIS = "pull.refresh.wait.min.millis";
  public static final String PULL_REFRESH_WAIT_MAX_MILLIS = "pull.refresh.wait.max.millis";

  public static final String POLL_REFRESH_TIMEOUT_MILLIS = "poll.refresh.timeout.millis";
  public static final String POLL_REFRESH_WAIT_MIN_MILLIS = "poll.refresh.wait.min.millis";
  public static final String POLL_REFRESH_WAIT_MAX_MILLIS = "poll.refresh.wait.max.millis";

  //@wjw_add for mongodb
  public static final String MONGODB_MANAGER_CLASS = "mongodbmanager.class";
  public static final String MONGODB_SERVERLIST = "mongodb.serverlist";
  public static final String MONGODB_AUTOCONNECTRETRY = "mongodb.autoConnectRetry";
  public static final String MONGODB_CONNECTIONSPERHOST = "mongodb.connectionsPerHost";
  public static final String MONGODB_THREADSALLOWEDTOBLOCKFORCONNECTIONMULTIPLIER = "mongodb.threadsAllowedToBlockForConnectionMultiplier";
  public static final String MONGODB_MAXWAITTIME = "mongodb.maxWaitTime";
  public static final String MONGODB_SLAVEOK = "mongodb.slaveOk";
  public static final String MONGODB_SOCKETKEEPALIVE = "mongodb.socketKeepAlive";
  public static final String MONGODB_SOCKETTIMEOUT = "mongodb.socketTimeout";
  public static final String MONGODB_CONNECTTIMEOUT = "mongodb.connectTimeout";
  public static final String MONGODB_SAFE = "mongodb.safe";
  public static final String MONGODB_W = "mongodb.w";
  public static final String MONGODB_WTIMEOUT = "mongodb.wtimeout";
  public static final String MONGODB_FSYNC = "mongodb.fsync";
  public static final String MONGODB_PAGESIZE = "mongodb.pagesize";

}
