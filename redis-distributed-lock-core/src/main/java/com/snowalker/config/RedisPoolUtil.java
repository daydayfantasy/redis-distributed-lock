package com.snowalker.config;

import java.util.List;
import redis.clients.jedis.Jedis;

/**
 * @author snowalker
 * @date 2018-7-9
 * @desc 封装单机版Jedis工具类
 */
public class RedisPoolUtil {

    private RedisPoolUtil(){}

    private static RedisPool redisPool;

    public static String get(String key){
        Jedis jedis = null;
        String result = null;
        try {
            jedis = RedisPool.getJedis();
            result = jedis.get(key);
        } catch (Exception e){
            e.printStackTrace();
        } finally {
            if (jedis != null) {
                jedis.close();
            }
            return result;
        }
    }

    public static Long setnx(String key, String value){
        Jedis jedis = null;
        Long result = null;
        try {
            jedis = RedisPool.getJedis();
            result = jedis.setnx(key, value);
        } catch (Exception e){
            e.printStackTrace();
        } finally {
            if (jedis != null) {
                jedis.close();
            }
            return result;
        }
    }

    public static String getSet(String key, String value){
        Jedis jedis = null;
        String result = null;
        try {
            jedis = RedisPool.getJedis();
            result = jedis.getSet(key, value);
        } catch (Exception e){
            e.printStackTrace();
        } finally {
            if (jedis != null) {
                jedis.close();
            }
            return result;
        }
    }

    public static Long expire(String key, int seconds){
        Jedis jedis = null;
        Long result = null;
        try {
            jedis = RedisPool.getJedis();
            result = jedis.expire(key, seconds);
        } catch (Exception e){
            e.printStackTrace();
        } finally {
            if (jedis != null) {
                jedis.close();
            }
            return result;
        }
    }

    /**
     * 设置带过期时间的键值对，原子操作
     * @param key 键
     * @param value 值
     * @param nxxx NX：只在键不存在时设置，XX：只在键存在时设置
     * @param expx EX：过期时间单位为秒，PX：过期时间单位为毫秒
     * @param time 过期时间
     * @return 设置成功返回OK，否则返回null
     */
    public static String set(String key, String value, String nxxx, String expx, long time){
        Jedis jedis = null;
        String result = null;
        try {
            jedis = RedisPool.getJedis();
            result = jedis.set(key, value, nxxx, expx, time);
        } catch (Exception e){
            e.printStackTrace();
        } finally {
            if (jedis != null) {
                jedis.close();
            }
            return result;
        }
    }

    /**
     * 执行Lua脚本
     * @param script Lua脚本
     * @param keys 键参数
     * @param args 值参数
     * @return 执行结果
     */
    public static Object eval(String script, List<String> keys, List<String> args){
        Jedis jedis = null;
        Object result = null;
        try {
            jedis = RedisPool.getJedis();
            result = jedis.eval(script, keys, args);
        } catch (Exception e){
            e.printStackTrace();
        } finally {
            if (jedis != null) {
                jedis.close();
            }
        }
        return result;
    }

    public static Long del(String key){
        Jedis jedis = null;
        Long result = null;
        try {
            jedis = RedisPool.getJedis();
            result = jedis.del(key);
        } catch (Exception e){
            e.printStackTrace();
        } finally {
            if (jedis != null) {
                jedis.close();
            }
            return result;
        }
    }
}
