package nl.justobjects.pushlet.mongodb;

import nl.justobjects.pushlet.core.Config;
import nl.justobjects.pushlet.core.ConfigDefs;
import nl.justobjects.pushlet.util.Log;

import com.mongodb.DB;
import com.mongodb.Mongo;
import com.mongodb.MongoOptions;
import com.mongodb.ServerAddress;

public class MongodbManager {
  protected static String serverlist = "127.0.0.1:27017"; //用逗号(,)分隔的"ip:port"列表
  
  public static Mongo _mongo;
  public static DB _db;

  /**
   * Singleton pattern: single instance.
   */
  private static MongodbManager instance;

  static {
    // Singleton + factory pattern:  create single instance
    // from configured class name
    try {
      instance = (MongodbManager) Config.getClass(ConfigDefs.MONGODB_MANAGER_CLASS, "nl.justobjects.pushlet.mongodb.MongodbManager").newInstance();
      Log.info("MongodbManager created className=" + instance.getClass());
      serverlist = Config.getProperty(ConfigDefs.MONGODB_SERVERLIST);
      String[] servers = serverlist.split(",");
      java.util.List<ServerAddress> replicaSetSeeds = new java.util.ArrayList<ServerAddress>(servers.length);
      for (int i = 0; i < servers.length; i++) {
        String[] hostAndPort = servers[i].split(":");
        ServerAddress shardInfo = new ServerAddress(hostAndPort[0], Integer.parseInt(hostAndPort[1]));
        replicaSetSeeds.add(shardInfo);
      }
      MongoOptions opt = new MongoOptions();
      opt.autoConnectRetry = Config.getBoolProperty(ConfigDefs.MONGODB_AUTOCONNECTRETRY);
      opt.connectionsPerHost = Config.getIntProperty(ConfigDefs.MONGODB_CONNECTIONSPERHOST);
      opt.threadsAllowedToBlockForConnectionMultiplier = Config.getIntProperty(ConfigDefs.MONGODB_THREADSALLOWEDTOBLOCKFORCONNECTIONMULTIPLIER);
      opt.maxWaitTime = Config.getIntProperty(ConfigDefs.MONGODB_MAXWAITTIME);
      opt.slaveOk = Config.getBoolProperty(ConfigDefs.MONGODB_SLAVEOK);
      opt.socketKeepAlive = Config.getBoolProperty(ConfigDefs.MONGODB_SOCKETKEEPALIVE);
      opt.socketTimeout = Config.getIntProperty(ConfigDefs.MONGODB_SOCKETTIMEOUT);
      opt.connectTimeout = Config.getIntProperty(ConfigDefs.MONGODB_CONNECTTIMEOUT);
      opt.safe = Config.getBoolProperty(ConfigDefs.MONGODB_SAFE);
      opt.w = Config.getIntProperty(ConfigDefs.MONGODB_W); //就是w来控制是否立即调用getLastError
      opt.wtimeout = Config.getIntProperty(ConfigDefs.MONGODB_WTIMEOUT);
      opt.fsync = Config.getBoolProperty(ConfigDefs.MONGODB_FSYNC); //fsync控制是否立即写磁盘

      _mongo = new Mongo(replicaSetSeeds, opt);
      _db = _mongo.getDB("pushlet");

      Log.info("replicaSetSeeds:" + replicaSetSeeds.toString());
      Log.info("初始化MongodbManager:" + instance.toString());
    } catch (Throwable t) {
      Log.fatal("Cannot instantiate MongodbManager from config", t);
    }
  }

  /**
   * Singleton pattern: protected constructor needed for derived classes.
   */
  protected MongodbManager() {
  }

  /**
   * Singleton pattern: get single instance.
   */
  public static MongodbManager getInstance() {
    return instance;
  }

  @Override
  public String toString() {
    return "MongodbManager{" + "serverlist=" + serverlist + ",mongo=" + _mongo + ",db=" + _db + '}';
  }

}
