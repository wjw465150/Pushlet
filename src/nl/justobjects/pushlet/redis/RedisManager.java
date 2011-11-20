package nl.justobjects.pushlet.redis;

import internal.com.thoughtworks.xstream.XStream;
import internal.com.thoughtworks.xstream.io.xml.XppDriver;
import internal.org.apache.commons.pool.impl.GenericObjectPool;

import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

import nl.justobjects.pushlet.core.Config;
import nl.justobjects.pushlet.core.ConfigDefs;
import nl.justobjects.pushlet.util.Log;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;
import redis.clients.jedis.JedisShardInfo;
import redis.clients.jedis.ShardedJedis;
import redis.clients.jedis.ShardedJedisPool;
import redis.clients.jedis.exceptions.JedisConnectionException;

public class RedisManager {
  public static final String REDIS_CHARSET = "UTF-8";
  static final XStream _xstream = new XStream(new XppDriver());
  static ShardedJedisPool _shardedPool = null;
  static JedisPool _pool = null;

  static private boolean debug = false; //是否打开调试模式
  static protected String serverlist = "127.0.0.1:6379"; //用逗号(,)分隔的"ip:port"列表
  static protected int minConn = 5;
  static protected int maxConn = 100;
  static protected int socketTO = 6000;

  /**
   * Singleton pattern: single instance.
   */
  private static RedisManager instance;

  static {
    // Singleton + factory pattern:  create single instance
    // from configured class name
    try {
      instance = (RedisManager) Config.getClass(ConfigDefs.REDIS_MANAGER_CLASS, "nl.justobjects.pushlet.redis.RedisManager").newInstance();
      Log.info("RedisManager created className=" + instance.getClass());
      debug = Config.getBoolProperty(ConfigDefs.REDIS_DEBUG);
      serverlist = Config.getProperty(ConfigDefs.REDIS_SERVERLIST);
      minConn = Config.getIntProperty(ConfigDefs.REDIS_MINCONN);
      maxConn = Config.getIntProperty(ConfigDefs.REDIS_MAXCONN);
      socketTO = Config.getIntProperty(ConfigDefs.REDIS_SOCKETTO);

      JedisPoolConfig poolConfig = new JedisPoolConfig();
      poolConfig.setMaxActive(maxConn);
      poolConfig.setMinIdle(minConn);
      int maxIdle = poolConfig.minIdle + 5;
      if (maxIdle > poolConfig.maxActive) {
        maxIdle = poolConfig.maxActive;
      }
      poolConfig.setMaxIdle(maxIdle);
      poolConfig.setMaxWait(1000L);
      poolConfig.setWhenExhaustedAction(GenericObjectPool.WHEN_EXHAUSTED_BLOCK);
      poolConfig.setTestOnBorrow(false);
      poolConfig.setTestOnReturn(false);
      poolConfig.setTestWhileIdle(true);
      poolConfig.setMinEvictableIdleTimeMillis(1000L * 60L * 10L); //空闲对象,空闲多长时间会被驱逐出池里
      poolConfig.setTimeBetweenEvictionRunsMillis(1000L * 30L); //驱逐线程30秒执行一次
      poolConfig.setNumTestsPerEvictionRun(-1); //-1,表示在驱逐线程执行时,测试所有的空闲对象

      String[] servers = serverlist.split(",");
      java.util.List<JedisShardInfo> shards = new java.util.ArrayList<JedisShardInfo>(servers.length);
      for (int i = 0; i < servers.length; i++) {
        String[] hostAndPort = servers[i].split(":");
        JedisShardInfo shardInfo = new JedisShardInfo(hostAndPort[0], Integer.parseInt(hostAndPort[1]), socketTO);
        if (hostAndPort.length == 3) {
          shardInfo.setPassword(hostAndPort[2]);
        }
        shards.add(shardInfo);
      }

      if (shards.size() == 1) {
        _pool = new JedisPool(poolConfig, shards.get(0).getHost(), shards.get(0).getPort(), shards.get(0).getTimeout(), shards.get(0).getPassword());
        Log.info("使用:JedisPool");
      } else {
        _shardedPool = new ShardedJedisPool(poolConfig, shards);
        Log.info("使用:ShardedJedisPool");
      }

      Log.info("RedisShards:" + shards.toString());
      Log.info("初始化RedisManager:" + instance.toString());
    } catch (Throwable t) {
      Log.fatal("Cannot instantiate RedisManager from config", t);
    }
  }

  /**
   * Singleton pattern: protected constructor needed for derived classes.
   */
  protected RedisManager() {
  }

  /**
   * Singleton pattern: get single instance.
   */
  public static RedisManager getInstance() {
    return instance;
  }

  @Override
  public String toString() {
    return "RedisManager{" + "debug=" + debug + ",serverlist=" + serverlist + ",minConn=" + minConn + ",maxConn="
        + maxConn + ",socketTO=" + socketTO + '}';
  }

  public String toXML(Object obj) {
    return _xstream.toXML(obj);
  }

  public Object fromXML(String xml) {
    return _xstream.fromXML(xml);
  }

  //基本操作
  public java.util.Set<byte[]> keys(String pattern) {
    if (_pool != null) {
      Jedis jedis = null;
      try {
        jedis = _pool.getResource();
        return jedis.keys(pattern.getBytes(REDIS_CHARSET));
      } catch (IOException e) {
        throw new JedisConnectionException(e);
      } finally {
        if (jedis != null) {
          try {
            _pool.returnResource(jedis);
          } catch (Throwable thex) {
          }
        }
      }
    } else {
      ShardedJedis jedis = null;
      try {
        jedis = _shardedPool.getResource();
        byte[] bytesKey = pattern.getBytes(REDIS_CHARSET);
        Jedis jedisA = jedis.getShard(bytesKey);
        return jedisA.keys(pattern.getBytes(REDIS_CHARSET));
      } catch (IOException e) {
        throw new JedisConnectionException(e);
      } finally {
        if (jedis != null) {
          try {
            _shardedPool.returnResource(jedis);
          } catch (Throwable thex) {
          }
        }
      }
    }
  }

  public String get(String key) {
    if (_pool != null) {
      Jedis jedis = null;
      try {
        jedis = _pool.getResource();
        byte[] byteValue = jedis.get(key.getBytes(REDIS_CHARSET));
        if (byteValue == null) {
          return null;
        }
        return new String(byteValue, REDIS_CHARSET);
      } catch (IOException e) {
        throw new JedisConnectionException(e);
      } finally {
        if (jedis != null) {
          try {
            _pool.returnResource(jedis);
          } catch (Throwable thex) {
          }
        }
      }
    } else {
      ShardedJedis jedis = null;
      try {
        jedis = _shardedPool.getResource();
        byte[] byteValue = jedis.get(key.getBytes(REDIS_CHARSET));
        if (byteValue == null) {
          return null;
        }
        return new String(byteValue, REDIS_CHARSET);
      } catch (IOException e) {
        throw new JedisConnectionException(e);
      } finally {
        if (jedis != null) {
          try {
            _shardedPool.returnResource(jedis);
          } catch (Throwable thex) {
          }
        }
      }
    }
  }

  public String setex(String key, int seconds, String value) {
    if (_pool != null) {
      Jedis jedis = null;
      try {
        jedis = _pool.getResource();
        return jedis.setex(key.getBytes(REDIS_CHARSET), seconds, value.getBytes(REDIS_CHARSET));
      } catch (IOException e) {
        throw new JedisConnectionException(e);
      } finally {
        if (jedis != null) {
          try {
            _pool.returnResource(jedis);
          } catch (Throwable thex) {
          }
        }
      }
    } else {
      ShardedJedis jedis = null;
      try {
        jedis = _shardedPool.getResource();
        return jedis.setex(key.getBytes(REDIS_CHARSET), seconds, value.getBytes(REDIS_CHARSET));
      } catch (IOException e) {
        throw new JedisConnectionException(e);
      } finally {
        if (jedis != null) {
          try {
            _shardedPool.returnResource(jedis);
          } catch (Throwable thex) {
          }
        }
      }
    }
  }

  public Long del(String key) {
    if (_pool != null) {
      Jedis jedis = null;
      try {
        jedis = _pool.getResource();
        return jedis.del(key.getBytes(REDIS_CHARSET));
      } catch (IOException e) {
        return 0L;
      } finally {
        if (jedis != null) {
          try {
            _pool.returnResource(jedis);
          } catch (Throwable thex) {
          }
        }
      }
    } else {
      ShardedJedis jedis = null;
      try {
        jedis = _shardedPool.getResource();
        byte[] bytesKey = key.getBytes(REDIS_CHARSET);
        Jedis jedisA = jedis.getShard(bytesKey);
        return jedisA.del(bytesKey);
      } catch (IOException e) {
        return 0L;
      } finally {
        if (jedis != null) {
          try {
            _shardedPool.returnResource(jedis);
          } catch (Throwable thex) {
          }
        }
      }
    }
  }

  public Boolean exists(String key) {
    if (_pool != null) {
      Jedis jedis = null;
      try {
        jedis = _pool.getResource();
        return jedis.exists(key.getBytes(REDIS_CHARSET));
      } catch (IOException e) {
        throw new JedisConnectionException(e);
      } finally {
        if (jedis != null) {
          try {
            _pool.returnResource(jedis);
          } catch (Throwable thex) {
          }
        }
      }
    } else {
      ShardedJedis jedis = null;
      try {
        jedis = _shardedPool.getResource();
        return jedis.exists(key.getBytes(REDIS_CHARSET));
      } catch (IOException e) {
        throw new JedisConnectionException(e);
      } finally {
        if (jedis != null) {
          try {
            _shardedPool.returnResource(jedis);
          } catch (Throwable thex) {
          }
        }
      }
    }

  }

  //Hash操作
  public String hget(String hkey, String field) {
    if (_pool != null) {
      Jedis jedis = null;
      try {
        jedis = _pool.getResource();
        byte[] byteValue = jedis.hget(hkey.getBytes(REDIS_CHARSET), field.getBytes(REDIS_CHARSET));
        if (byteValue == null) {
          return null;
        }
        return new String(byteValue, REDIS_CHARSET);
      } catch (IOException e) {
        throw new JedisConnectionException(e);
      } finally {
        if (jedis != null) {
          try {
            _pool.returnResource(jedis);
          } catch (Throwable thex) {
          }
        }
      }
    } else {
      ShardedJedis jedis = null;
      try {
        jedis = _shardedPool.getResource();
        byte[] byteValue = jedis.hget(hkey.getBytes(REDIS_CHARSET), field.getBytes(REDIS_CHARSET));
        if (byteValue == null) {
          return null;
        }
        return new String(byteValue, REDIS_CHARSET);
      } catch (IOException e) {
        throw new JedisConnectionException(e);
      } finally {
        if (jedis != null) {
          try {
            _shardedPool.returnResource(jedis);
          } catch (Throwable thex) {
          }
        }
      }
    }
  }

  public Long hset(String hkey, String field, String value) {
    if (_pool != null) {
      Jedis jedis = null;
      try {
        jedis = _pool.getResource();
        return jedis.hset(hkey.getBytes(REDIS_CHARSET), field.getBytes(REDIS_CHARSET), value.getBytes(REDIS_CHARSET));
      } catch (IOException e) {
        throw new JedisConnectionException(e);
      } finally {
        if (jedis != null) {
          try {
            _pool.returnResource(jedis);
          } catch (Throwable thex) {
          }
        }
      }
    } else {
      ShardedJedis jedis = null;
      try {
        jedis = _shardedPool.getResource();
        return jedis.hset(hkey.getBytes(REDIS_CHARSET), field.getBytes(REDIS_CHARSET), value.getBytes(REDIS_CHARSET));
      } catch (IOException e) {
        throw new JedisConnectionException(e);
      } finally {
        if (jedis != null) {
          try {
            _shardedPool.returnResource(jedis);
          } catch (Throwable thex) {
          }
        }
      }
    }
  }

  public Long hdel(String hkey, String field) {
    if (_pool != null) {
      Jedis jedis = null;
      try {
        jedis = _pool.getResource();
        return jedis.hdel(hkey.getBytes(REDIS_CHARSET), field.getBytes(REDIS_CHARSET));
      } catch (IOException e) {
        throw new JedisConnectionException(e);
      } finally {
        if (jedis != null) {
          try {
            _pool.returnResource(jedis);
          } catch (Throwable thex) {
          }
        }
      }
    } else {
      ShardedJedis jedis = null;
      try {
        jedis = _shardedPool.getResource();
        return jedis.hdel(hkey.getBytes(REDIS_CHARSET), field.getBytes(REDIS_CHARSET));
      } catch (IOException e) {
        throw new JedisConnectionException(e);
      } finally {
        if (jedis != null) {
          try {
            _shardedPool.returnResource(jedis);
          } catch (Throwable thex) {
          }
        }
      }
    }
  }

  public java.util.Map<byte[], byte[]> hgetAll(String hkey) {
    if (_pool != null) {
      Jedis jedis = null;
      try {
        jedis = _pool.getResource();
        return jedis.hgetAll(hkey.getBytes(REDIS_CHARSET));
      } catch (IOException e) {
        throw new JedisConnectionException(e);
      } finally {
        if (jedis != null) {
          try {
            _pool.returnResource(jedis);
          } catch (Throwable thex) {
          }
        }
      }
    } else {
      ShardedJedis jedis = null;
      try {
        jedis = _shardedPool.getResource();
        return jedis.hgetAll(hkey.getBytes(REDIS_CHARSET));
      } catch (IOException e) {
        throw new JedisConnectionException(e);
      } finally {
        if (jedis != null) {
          try {
            _shardedPool.returnResource(jedis);
          } catch (Throwable thex) {
          }
        }
      }
    }
  }

  public String hmset(String hkey, java.util.Map<String, String> hash) {
    if (_pool != null) {
      Jedis jedis = null;
      try {
        jedis = _pool.getResource();

        Map<byte[], byte[]> bhash = new HashMap<byte[], byte[]>(hash.size());
        Map.Entry<String, String> entry;
        Iterator<Map.Entry<String, String>> iterator = hash.entrySet().iterator();
        while (iterator.hasNext()) {
          entry = iterator.next();
          bhash.put(entry.getKey().getBytes(REDIS_CHARSET), entry.getValue().getBytes(REDIS_CHARSET));
        }

        return jedis.hmset(hkey.getBytes(REDIS_CHARSET), bhash);
      } catch (IOException e) {
        throw new JedisConnectionException(e);
      } finally {
        if (jedis != null) {
          try {
            _pool.returnResource(jedis);
          } catch (Throwable thex) {
          }
        }
      }
    } else {
      ShardedJedis jedis = null;
      try {
        jedis = _shardedPool.getResource();

        Map<byte[], byte[]> bhash = new HashMap<byte[], byte[]>(hash.size());
        Map.Entry<String, String> entry;
        Iterator<Map.Entry<String, String>> iterator = hash.entrySet().iterator();
        while (iterator.hasNext()) {
          entry = iterator.next();
          bhash.put(entry.getKey().getBytes(REDIS_CHARSET), entry.getValue().getBytes(REDIS_CHARSET));
        }

        return jedis.hmset(hkey.getBytes(REDIS_CHARSET), bhash);
      } catch (IOException e) {
        throw new JedisConnectionException(e);
      } finally {
        if (jedis != null) {
          try {
            _shardedPool.returnResource(jedis);
          } catch (Throwable thex) {
          }
        }
      }
    }

  }

  public Long hlen(String hkey) {
    if (_pool != null) {
      Jedis jedis = null;
      try {
        jedis = _pool.getResource();
        return jedis.hlen(hkey.getBytes(REDIS_CHARSET));
      } catch (IOException e) {
        throw new JedisConnectionException(e);
      } finally {
        if (jedis != null) {
          try {
            _pool.returnResource(jedis);
          } catch (Throwable thex) {
          }
        }
      }
    } else {
      ShardedJedis jedis = null;
      try {
        jedis = _shardedPool.getResource();
        return jedis.hlen(hkey.getBytes(REDIS_CHARSET));
      } catch (IOException e) {
        throw new JedisConnectionException(e);
      } finally {
        if (jedis != null) {
          try {
            _shardedPool.returnResource(jedis);
          } catch (Throwable thex) {
          }
        }
      }
    }
  }

  public java.util.Set<byte[]> hkeys(String hkey) {
    if (_pool != null) {
      Jedis jedis = null;
      try {
        jedis = _pool.getResource();
        return jedis.hkeys(hkey.getBytes(REDIS_CHARSET));
      } catch (IOException e) {
        throw new JedisConnectionException(e);
      } finally {
        if (jedis != null) {
          try {
            _pool.returnResource(jedis);
          } catch (Throwable thex) {
          }
        }
      }
    } else {
      ShardedJedis jedis = null;
      try {
        jedis = _shardedPool.getResource();
        byte[] bytesKey = hkey.getBytes(REDIS_CHARSET);
        Jedis jedisA = jedis.getShard(bytesKey);
        return jedisA.hkeys(bytesKey);
      } catch (IOException e) {
        throw new JedisConnectionException(e);
      } finally {
        if (jedis != null) {
          try {
            _shardedPool.returnResource(jedis);
          } catch (Throwable thex) {
          }
        }
      }
    }
  }

  public java.util.List<byte[]> hvals(String hkey) {
    if (_pool != null) {
      Jedis jedis = null;
      try {
        jedis = _pool.getResource();
        return jedis.hvals(hkey.getBytes(REDIS_CHARSET));
      } catch (IOException e) {
        throw new JedisConnectionException(e);
      } finally {
        if (jedis != null) {
          try {
            _pool.returnResource(jedis);
          } catch (Throwable thex) {
          }
        }
      }
    } else {
      ShardedJedis jedis = null;
      try {
        jedis = _shardedPool.getResource();
        byte[] bytesKey = hkey.getBytes(REDIS_CHARSET);
        Jedis jedisA = jedis.getShard(bytesKey);
        return jedisA.hvals(bytesKey);
      } catch (IOException e) {
        throw new JedisConnectionException(e);
      } finally {
        if (jedis != null) {
          try {
            _shardedPool.returnResource(jedis);
          } catch (Throwable thex) {
          }
        }
      }
    }
  }

  public Boolean hexists(String hkey, String field) {
    if (_pool != null) {
      Jedis jedis = null;
      try {
        jedis = _pool.getResource();
        return jedis.hexists(hkey.getBytes(REDIS_CHARSET), field.getBytes(REDIS_CHARSET));
      } catch (IOException e) {
        throw new JedisConnectionException(e);
      } finally {
        if (jedis != null) {
          try {
            _pool.returnResource(jedis);
          } catch (Throwable thex) {
          }
        }
      }
    } else {
      ShardedJedis jedis = null;
      try {
        jedis = _shardedPool.getResource();
        return jedis.hexists(hkey.getBytes(REDIS_CHARSET), field.getBytes(REDIS_CHARSET));
      } catch (IOException e) {
        throw new JedisConnectionException(e);
      } finally {
        if (jedis != null) {
          try {
            _shardedPool.returnResource(jedis);
          } catch (Throwable thex) {
          }
        }
      }
    }
  }

  //List操作
  public Long lpush(String lkey, String value) {
    if (_pool != null) {
      Jedis jedis = null;
      try {
        jedis = _pool.getResource();
        return jedis.lpush(lkey.getBytes(REDIS_CHARSET), value.getBytes(REDIS_CHARSET));
      } catch (IOException e) {
        throw new JedisConnectionException(e);
      } finally {
        if (jedis != null) {
          try {
            _pool.returnResource(jedis);
          } catch (Throwable thex) {
          }
        }
      }
    } else {
      ShardedJedis jedis = null;
      try {
        jedis = _shardedPool.getResource();
        return jedis.lpush(lkey.getBytes(REDIS_CHARSET), value.getBytes(REDIS_CHARSET));
      } catch (IOException e) {
        throw new JedisConnectionException(e);
      } finally {
        if (jedis != null) {
          try {
            _shardedPool.returnResource(jedis);
          } catch (Throwable thex) {
          }
        }
      }
    }
  }

  public String lpop(String lkey) {
    if (_pool != null) {
      Jedis jedis = null;
      try {
        jedis = _pool.getResource();
        byte[] byteValue = jedis.lpop(lkey.getBytes(REDIS_CHARSET));
        if (byteValue == null) {
          return null;
        }
        return new String(byteValue, REDIS_CHARSET);
      } catch (IOException e) {
        throw new JedisConnectionException(e);
      } finally {
        if (jedis != null) {
          try {
            _pool.returnResource(jedis);
          } catch (Throwable thex) {
          }
        }
      }
    } else {
      ShardedJedis jedis = null;
      try {
        jedis = _shardedPool.getResource();
        byte[] byteValue = jedis.lpop(lkey.getBytes(REDIS_CHARSET));
        if (byteValue == null) {
          return null;
        }
        return new String(byteValue, REDIS_CHARSET);
      } catch (IOException e) {
        throw new JedisConnectionException(e);
      } finally {
        if (jedis != null) {
          try {
            _shardedPool.returnResource(jedis);
          } catch (Throwable thex) {
          }
        }
      }
    }
  }

  public String blpop(int timeout, String lkey) {
    if (_pool != null) {
      Jedis jedis = null;
      try {
        jedis = _pool.getResource();
        java.util.List<byte[]> list = jedis.blpop(timeout, lkey.getBytes(REDIS_CHARSET));
        if (list == null) {
          return null;
        }
        return new String(list.get(1), REDIS_CHARSET);
      } catch (IOException e) {
        throw new JedisConnectionException(e);
      } finally {
        if (jedis != null) {
          try {
            _pool.returnResource(jedis);
          } catch (Throwable thex) {
          }
        }
      }
    } else {
      ShardedJedis jedis = null;
      try {
        jedis = _shardedPool.getResource();
        byte[] bytesKey = lkey.getBytes(REDIS_CHARSET);
        Jedis jedisA = jedis.getShard(bytesKey);
        java.util.List<byte[]> list = jedisA.blpop(timeout, bytesKey);
        if (list == null) {
          return null;
        }
        return new String(list.get(1), REDIS_CHARSET);
      } catch (IOException e) {
        throw new JedisConnectionException(e);
      } finally {
        if (jedis != null) {
          try {
            _shardedPool.returnResource(jedis);
          } catch (Throwable thex) {
          }
        }
      }
    }
  }

  public Long llen(String lkey) {
    if (_pool != null) {
      Jedis jedis = null;
      try {
        jedis = _pool.getResource();
        return jedis.llen(lkey.getBytes(REDIS_CHARSET));
      } catch (IOException e) {
        throw new JedisConnectionException(e);
      } finally {
        if (jedis != null) {
          try {
            _pool.returnResource(jedis);
          } catch (Throwable thex) {
          }
        }
      }
    } else {
      ShardedJedis jedis = null;
      try {
        jedis = _shardedPool.getResource();
        return jedis.llen(lkey.getBytes(REDIS_CHARSET));
      } catch (IOException e) {
        throw new JedisConnectionException(e);
      } finally {
        if (jedis != null) {
          try {
            _shardedPool.returnResource(jedis);
          } catch (Throwable thex) {
          }
        }
      }
    }
  }

  public java.util.List<byte[]> lrange(java.lang.String lkey, int start, int end) {
    if (_pool != null) {
      Jedis jedis = null;
      try {
        jedis = _pool.getResource();
        return jedis.lrange(lkey.getBytes(REDIS_CHARSET), start, end);
      } catch (IOException e) {
        throw new JedisConnectionException(e);
      } finally {
        if (jedis != null) {
          try {
            _pool.returnResource(jedis);
          } catch (Throwable thex) {
          }
        }
      }
    } else {
      ShardedJedis jedis = null;
      try {
        jedis = _shardedPool.getResource();
        return jedis.lrange(lkey.getBytes(REDIS_CHARSET), start, end);
      } catch (IOException e) {
        throw new JedisConnectionException(e);
      } finally {
        if (jedis != null) {
          try {
            _shardedPool.returnResource(jedis);
          } catch (Throwable thex) {
          }
        }
      }
    }
  }

  public Long lrem(String lkey, int count, String value) {
    if (_pool != null) {
      Jedis jedis = null;
      try {
        jedis = _pool.getResource();
        return jedis.lrem(lkey.getBytes(REDIS_CHARSET), count, value.getBytes(REDIS_CHARSET));
      } catch (IOException e) {
        throw new JedisConnectionException(e);
      } finally {
        if (jedis != null) {
          try {
            _pool.returnResource(jedis);
          } catch (Throwable thex) {
          }
        }
      }
    } else {
      ShardedJedis jedis = null;
      try {
        jedis = _shardedPool.getResource();
        return jedis.lrem(lkey.getBytes(REDIS_CHARSET), count, value.getBytes(REDIS_CHARSET));
      } catch (IOException e) {
        throw new JedisConnectionException(e);
      } finally {
        if (jedis != null) {
          try {
            _shardedPool.returnResource(jedis);
          } catch (Throwable thex) {
          }
        }
      }
    }
  }

  //Set操作
  public Boolean sismember(String skey, String member) {
    if (_pool != null) {
      Jedis jedis = null;
      try {
        jedis = _pool.getResource();
        return jedis.sismember(skey.getBytes(REDIS_CHARSET), member.getBytes(REDIS_CHARSET));
      } catch (IOException e) {
        throw new JedisConnectionException(e);
      } finally {
        if (jedis != null) {
          try {
            _pool.returnResource(jedis);
          } catch (Throwable thex) {
          }
        }
      }
    } else {
      ShardedJedis jedis = null;
      try {
        jedis = _shardedPool.getResource();
        return jedis.sismember(skey.getBytes(REDIS_CHARSET), member.getBytes(REDIS_CHARSET));
      } catch (IOException e) {
        throw new JedisConnectionException(e);
      } finally {
        if (jedis != null) {
          try {
            _shardedPool.returnResource(jedis);
          } catch (Throwable thex) {
          }
        }
      }
    }
  }

  public Long sadd(String skey, String member) {
    if (_pool != null) {
      Jedis jedis = null;
      try {
        jedis = _pool.getResource();
        return jedis.sadd(skey.getBytes(REDIS_CHARSET), member.getBytes(REDIS_CHARSET));
      } catch (IOException e) {
        throw new JedisConnectionException(e);
      } finally {
        if (jedis != null) {
          try {
            _pool.returnResource(jedis);
          } catch (Throwable thex) {
          }
        }
      }
    } else {
      ShardedJedis jedis = null;
      try {
        jedis = _shardedPool.getResource();
        return jedis.sadd(skey.getBytes(REDIS_CHARSET), member.getBytes(REDIS_CHARSET));
      } catch (IOException e) {
        throw new JedisConnectionException(e);
      } finally {
        if (jedis != null) {
          try {
            _shardedPool.returnResource(jedis);
          } catch (Throwable thex) {
          }
        }
      }
    }
  }

  public Long srem(String skey, String member) {
    if (_pool != null) {
      Jedis jedis = null;
      try {
        jedis = _pool.getResource();
        return jedis.srem(skey.getBytes(REDIS_CHARSET), member.getBytes(REDIS_CHARSET));
      } catch (IOException e) {
        throw new JedisConnectionException(e);
      } finally {
        if (jedis != null) {
          try {
            _pool.returnResource(jedis);
          } catch (Throwable thex) {
          }
        }
      }
    } else {
      ShardedJedis jedis = null;
      try {
        jedis = _shardedPool.getResource();
        return jedis.srem(skey.getBytes(REDIS_CHARSET), member.getBytes(REDIS_CHARSET));
      } catch (IOException e) {
        throw new JedisConnectionException(e);
      } finally {
        if (jedis != null) {
          try {
            _shardedPool.returnResource(jedis);
          } catch (Throwable thex) {
          }
        }
      }
    }
  }
}
