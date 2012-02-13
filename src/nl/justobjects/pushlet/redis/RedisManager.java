package nl.justobjects.pushlet.redis;

import internal.org.apache.commons.pool.impl.GenericObjectPool;

import java.io.IOException;

import nl.justobjects.pushlet.core.Config;
import nl.justobjects.pushlet.core.ConfigDefs;
import nl.justobjects.pushlet.util.Log;

import org.codehaus.jackson.JsonGenerator;
import org.codehaus.jackson.JsonNode;
import org.codehaus.jackson.JsonParser;
import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jackson.node.JsonNodeFactory;
import org.codehaus.jackson.node.ObjectNode;

import internal.redis.clients.jedis.Jedis;
import internal.redis.clients.jedis.JedisPool;
import internal.redis.clients.jedis.JedisPoolConfig;
import internal.redis.clients.jedis.JedisShardInfo;
import internal.redis.clients.jedis.ShardedJedis;
import internal.redis.clients.jedis.ShardedJedisPool;
import internal.redis.clients.jedis.exceptions.JedisConnectionException;

public class RedisManager {
  public static final String REDIS_CHARSET = "UTF-8";
  //static final XStream _xstream = new XStream(new XppDriver());
  static final ObjectMapper _mapper = new ObjectMapper(); //@wjw_comment ObjectMapper是线程安全的

  static ShardedJedisPool _shardedPool = null;
  static JedisPool _pool = null;

  static private boolean debug = false; //是否打开调试模式
  static protected String serverlist = "127.0.0.1:6379"; //用逗号(,)分隔的"ip:port"列表
  static protected int minConn = 5;
  static protected int maxConn = 100;
  static protected int socketTO = 6000;
  public static int pagesize = 100;

  /**
   * Singleton pattern: single instance.
   */
  private static RedisManager instance;

  static {
    try {
      //->初始化Json
      _mapper.configure(JsonGenerator.Feature.QUOTE_FIELD_NAMES, true);

      _mapper.configure(JsonParser.Feature.ALLOW_COMMENTS, true);
      _mapper.configure(JsonParser.Feature.ALLOW_SINGLE_QUOTES, true);
      _mapper.configure(JsonParser.Feature.ALLOW_UNQUOTED_FIELD_NAMES, true);
      //<-初始化Json

      instance = (RedisManager) Config.getClass(ConfigDefs.REDIS_MANAGER_CLASS, "nl.justobjects.pushlet.redis.RedisManager").newInstance();
      Log.info("RedisManager created className=" + instance.getClass());
      debug = Config.getBoolProperty(ConfigDefs.REDIS_DEBUG);
      serverlist = Config.getProperty(ConfigDefs.REDIS_SERVERLIST);
      minConn = Config.getIntProperty(ConfigDefs.REDIS_MINCONN);
      maxConn = Config.getIntProperty(ConfigDefs.REDIS_MAXCONN);
      socketTO = Config.getIntProperty(ConfigDefs.REDIS_SOCKETTO);
      pagesize = Config.getIntProperty(ConfigDefs.REDIS_PAGESIZE) - 1;

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
      t.printStackTrace();
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

  //  public String toXML(Object obj) {
  //    return _xstream.toXML(obj);
  //  }
  //
  //  public Object fromXML(String xml) {
  //    return _xstream.fromXML(xml);
  //  }

  public JsonNode readTree(String content) {
    try {
      return _mapper.readTree(content);
    } catch (IOException e) {
      return new ObjectNode(JsonNodeFactory.instance);
    }
  }

  public String objToJsonString(Object obj) {
    try {
      return _mapper.writeValueAsString(obj);
    } catch (Exception e) {
      return "{}";
    }
  }

  public <T> T jsonStringToObj(String content, Class<T> classType) {
    try {
      return _mapper.readValue(content, classType);
    } catch (Exception e) {
      return null;
    }
  }

  //TODO@redis的基本操作
  public java.util.Set<String> keys(String pattern) {
    if (_pool != null) {
      Jedis jedis = null;
      try {
        jedis = _pool.getResource();
        return jedis.keys(pattern);
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
        return jedisA.keys(pattern);
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
        return jedis.get(key);
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
        return jedis.get(key);
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
        return jedis.setex(key, seconds, value);
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
        return jedis.setex(key, seconds, value);
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
        return jedis.del(key);
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
        return jedis.del(key);
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
        return jedis.exists(key);
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
        return jedis.exists(key);
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

  //TODO@Hash操作
  public String hget(String hkey, String field) {
    if (_pool != null) {
      Jedis jedis = null;
      try {
        jedis = _pool.getResource();
        return jedis.hget(hkey, field);
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
        return jedis.hget(hkey, field);
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
        return jedis.hset(hkey, field, value);
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
        return jedis.hset(hkey, field, value);
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

  public Long hsetnx(String hkey, String field, String value) {
    if (_pool != null) {
      Jedis jedis = null;
      try {
        jedis = _pool.getResource();
        return jedis.hsetnx(hkey, field, value);
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
        return jedis.hsetnx(hkey, field, value);
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
        return jedis.hdel(hkey, field);
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
        return jedis.hdel(hkey, field);
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

  public java.util.Map<String, String> hgetAll(String hkey) {
    if (_pool != null) {
      Jedis jedis = null;
      try {
        jedis = _pool.getResource();
        return jedis.hgetAll(hkey);
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
        return jedis.hgetAll(hkey);
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

        return jedis.hmset(hkey, hash);
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

        return jedis.hmset(hkey, hash);
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
        return jedis.hlen(hkey);
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
        return jedis.hlen(hkey);
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

  public java.util.Set<String> hkeys(String hkey) {
    if (_pool != null) {
      Jedis jedis = null;
      try {
        jedis = _pool.getResource();
        return jedis.hkeys(hkey);
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
        return jedis.hkeys(hkey);
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

  public java.util.List<String> hvals(String hkey) {
    if (_pool != null) {
      Jedis jedis = null;
      try {
        jedis = _pool.getResource();
        return jedis.hvals(hkey);
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
        return jedis.hvals(hkey);
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
        return jedis.hexists(hkey, field);
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
        return jedis.hexists(hkey, field);
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

  //TODO@List操作
  public Long lpush(String lkey, String value) {
    if (_pool != null) {
      Jedis jedis = null;
      try {
        jedis = _pool.getResource();
        return jedis.lpush(lkey, value);
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
        return jedis.lpush(lkey, value);
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
        return jedis.lpop(lkey);
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
        return jedis.lpop(lkey);
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
        return jedis.llen(lkey);
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
        return jedis.llen(lkey);
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

  public java.util.List<String> lrange(String lkey, int start, int end) {
    if (_pool != null) {
      Jedis jedis = null;
      try {
        jedis = _pool.getResource();
        return jedis.lrange(lkey, start, end);
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
        return jedis.lrange(lkey, start, end);
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
        return jedis.lrem(lkey, count, value);
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
        return jedis.lrem(lkey, count, value);
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

  //TODO@Set操作
  public Boolean sismember(String skey, String member) {
    if (_pool != null) {
      Jedis jedis = null;
      try {
        jedis = _pool.getResource();
        return jedis.sismember(skey, member);
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
        return jedis.sismember(skey, member);
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
        return jedis.sadd(skey, member);
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
        return jedis.sadd(skey, member);
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
        return jedis.srem(skey, member);
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
        return jedis.srem(skey, member);
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

  //TODO@sort_set
  public Long zadd(String zkey, double score, String member) {
    if (_pool != null) {
      Jedis jedis = null;
      try {
        jedis = _pool.getResource();
        return jedis.zadd(zkey, score, member);
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
        return jedis.zadd(zkey, score, member);
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

  public Long zrem(String zkey, String member) {
    if (_pool != null) {
      Jedis jedis = null;
      try {
        jedis = _pool.getResource();
        return jedis.zrem(zkey, member);
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
        return jedis.zrem(zkey, member);
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

  public Double zscore(String zkey, String member) {
    if (_pool != null) {
      Jedis jedis = null;
      try {
        jedis = _pool.getResource();
        return jedis.zscore(zkey, member);
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
        return jedis.zscore(zkey, member);
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

  public java.util.Set<String> zrange(String zkey, int start, int end) {
    if (_pool != null) {
      Jedis jedis = null;
      try {
        jedis = _pool.getResource();
        return jedis.zrange(zkey, start, end);
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
        return jedis.zrange(zkey, start, end);
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
