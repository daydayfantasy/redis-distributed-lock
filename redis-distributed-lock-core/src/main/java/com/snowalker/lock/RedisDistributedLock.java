package com.snowalker.lock;

import com.snowalker.config.RedisPoolUtil;
import com.snowalker.util.PropertiesUtil;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.util.Arrays;
import java.util.List;

/**
 * @author snowalker
 * @date 2018-7-9
 * @desc redis分布式锁核心实现
 */
public class RedisDistributedLock implements DistributedLock {

    /**默认锁超时时间为10S*/
    private static final int EXPIRE_SECONDS = 50;
    private static final Logger log = LoggerFactory.getLogger(RedisDistributedLock.class);

    private RedisDistributedLock() {
    }

    private volatile static RedisDistributedLock redisDistributedLock;

    public static RedisDistributedLock getInstance() {
        if (redisDistributedLock == null) {
            synchronized (RedisDistributedLock.class) {
                if (redisDistributedLock == null) {
                    redisDistributedLock = new RedisDistributedLock();
                }
            }
        }
        return redisDistributedLock;
    }

    /**
     * 加锁
     *
     * @param lockName
     * @return 返回true表示加锁成功，执行业务逻辑，执行完毕需要主动释放锁，否则就需要等待锁超时重新争抢
     * 返回false标识加锁失败，阻塞并继续尝试获取锁
     */
    @Override
    public boolean lock(String lockName) {
        /**1.使用setNx开始加锁*/
        log.info("开始获取Redis分布式锁流程,lockName={},CurrentThreadName={}", lockName, Thread.currentThread().getName());
        long lockTimeout = Long.parseLong(PropertiesUtil.getProperty("redis.lock.timeout", "5"));
        /**redis中锁的值为:当前时间+超时时间*/
        String lockValue = String.valueOf(System.currentTimeMillis() + lockTimeout);
        // 使用原子操作set(key, value, NX, EX, time)替代setnx+expire
        String lockResult = RedisPoolUtil.set(lockName, lockValue, "NX", "EX", EXPIRE_SECONDS);

        if ("OK".equals(lockResult)) {
            log.info("原子set获取分布式锁[成功],threadName={}", Thread.currentThread().getName());
            return true;
        } else {
            log.info("原子set获取分布式锁[失败],threadName={}", Thread.currentThread().getName());
            return tryLock(lockName, lockTimeout);
        }
    }

    private boolean tryLock(String lockName, long lockTimeout) {
        /**
         * 2.加锁失败后再次尝试
         * 2.1获取锁失败，继续判断，判断时间戳，看是否可以重置并获取到锁
         *    setNx结果小于当前时间，表明锁已过期，可以再次尝试加锁
         */
        String lockValueStr = RedisPoolUtil.get(lockName);
        if (lockValueStr != null) {
            Long lockValueATime = Long.parseLong(lockValueStr);
            log.info("lockValueATime为:" + lockValueATime);
            if (lockValueATime < System.currentTimeMillis()) {
                // 使用Lua脚本确保getset和expire操作的原子性
                String luaScript = "local current_value = redis.call('GET', KEYS[1])\n" +
                        "if current_value == false or tonumber(current_value) < tonumber(ARGV[1]) then\n" +
                        "    local old_value = redis.call('GETSET', KEYS[1], ARGV[2])\n" +
                        "    if old_value == false or old_value == current_value then\n" +
                        "        redis.call('EXPIRE', KEYS[1], ARGV[3])\n" +
                        "        return 1\n" +
                        "    end\n" +
                        "end\n" +
                        "return 0";
                
                List<String> keys = Arrays.asList(lockName);
                List<String> args = Arrays.asList(String.valueOf(System.currentTimeMillis()), 
                                                String.valueOf(System.currentTimeMillis() + lockTimeout), 
                                                String.valueOf(EXPIRE_SECONDS));
                
                Object result = RedisPoolUtil.eval(luaScript, keys, args);
                log.info("Lua脚本执行结果为:" + result);
                
                if (result != null && "1".equals(result.toString())) {
                    log.info("获取Redis分布式锁[成功],lockName={},CurrentThreadName={}",
                            lockName, Thread.currentThread().getName());
                    return true;
                } else {
                    log.info("获取锁失败,lockName={},CurrentThreadName={}",
                            lockName, Thread.currentThread().getName());
                    return false;
                }
            }
        }
        /**3.锁未超时，获取锁失败*/
        log.info("当前锁未失效！！！！，竞争失败，继续持有之前的锁,lockName={},CurrentThreadName={}",
                lockName, Thread.currentThread().getName());
        return false;
    }

    /**
     * 解锁
     *
     * @param lockName
     */
    @Override
    public boolean release(String lockName) {
        // 使用Lua脚本确保检查锁持有者和删除锁的原子性
        String luaScript = "local current_value = redis.call('GET', KEYS[1])\n" +
                "if current_value == ARGV[1] then\n" +
                "    return redis.call('DEL', KEYS[1])\n" +
                "end\n" +
                "return 0";
        
        // 获取当前线程持有的锁值
        String lockValue = RedisPoolUtil.get(lockName);
        if (lockValue == null) {
            log.info("Redis分布式锁不存在，无需释放, key= :{}", lockName);
            return true;
        }
        
        List<String> keys = Arrays.asList(lockName);
        List<String> args = Arrays.asList(lockValue);
        
        Object result = RedisPoolUtil.eval(luaScript, keys, args);
        log.info("释放Redis分布式锁结果为:{}, key= :{}", result, lockName);
        
        return result != null && "1".equals(result.toString());
    }
}
