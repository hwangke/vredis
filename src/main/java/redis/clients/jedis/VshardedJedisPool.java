package redis.clients.jedis;

import org.apache.commons.pool2.PooledObject;
import org.apache.commons.pool2.PooledObjectFactory;
import org.apache.commons.pool2.impl.AbandonedConfig;
import org.apache.commons.pool2.impl.DefaultPooledObject;
import org.apache.commons.pool2.impl.GenericObjectPool;
import org.apache.commons.pool2.impl.GenericObjectPoolConfig;
import redis.clients.util.Hashing;
import redis.clients.util.Pool;

import java.util.List;
import java.util.regex.Pattern;


public class VshardedJedisPool extends Pool<VshardedJedis> {



    public VshardedJedisPool(final GenericObjectPoolConfig poolConfig,
                             List<JedisShardInfo> shards,AbandonedConfig abandonedConfig) {
        this(poolConfig, shards, Hashing.MURMUR_HASH,abandonedConfig);
    }

    public VshardedJedisPool(final GenericObjectPoolConfig poolConfig,
                             List<JedisShardInfo> shards, Hashing algo,AbandonedConfig abandonedConfig) {
        this(poolConfig, shards, algo, null,abandonedConfig);
    }

    public VshardedJedisPool(final GenericObjectPoolConfig poolConfig,
                             List<JedisShardInfo> shards, Pattern keyTagPattern,AbandonedConfig abandonedConfig) {
        this(poolConfig, shards, Hashing.MURMUR_HASH, keyTagPattern,abandonedConfig);
    }

    public VshardedJedisPool(final GenericObjectPoolConfig poolConfig,
                             List<JedisShardInfo> shards, Hashing algo, Pattern keyTagPattern,AbandonedConfig abandonedConfig) {
        super(poolConfig, new ShardedJedisFactory(shards, algo, keyTagPattern));
        ShardedJedisFactory.pool = this.internalPool;
        internalPool.setAbandonedConfig(abandonedConfig);
    }

    /**
     * PoolableObjectFactory custom impl.
     */
    private static class ShardedJedisFactory implements  PooledObjectFactory<VshardedJedis> {
        private List<JedisShardInfo> shards;
        private Hashing algo;
        private Pattern keyTagPattern;
        private static GenericObjectPool pool;

        public ShardedJedisFactory(List<JedisShardInfo> shards, Hashing algo,
                                   Pattern keyTagPattern) {
            this.shards = shards;
            this.algo = algo;
            this.keyTagPattern = keyTagPattern;
        }

        @Override
        public PooledObject<VshardedJedis> makeObject() throws Exception {
            VshardedJedis jedis = new VshardedJedis(shards, algo, keyTagPattern);
            return new DefaultPooledObject<VshardedJedis>(jedis);
        }

        @Override
        public void destroyObject(PooledObject<VshardedJedis> pooledShardedJedis)
                throws Exception {
            final VshardedJedis shardedJedis = pooledShardedJedis.getObject();
            for (Jedis jedis : shardedJedis.getAllShards()) {
                try {
                    try {
                        jedis.quit();
                    } catch (Exception e) {

                    }
                    jedis.disconnect();
                } catch (Exception e) {

                }
            }
        }

        @Override
        public boolean validateObject(
                PooledObject<VshardedJedis> pooledShardedJedis) {
            try {
                VshardedJedis jedis = pooledShardedJedis.getObject();
                for (Jedis shard : jedis.getAllShards()) {
                    if (!shard.ping().equals("PONG")) {
                        return false;
                    }
                }
                return true;
            } catch (Exception ex) {
                return false;
            }
        }

        @Override
        public void activateObject(PooledObject<VshardedJedis> p)
                throws Exception {
        }

        @Override
        public void passivateObject(PooledObject<VshardedJedis> p)
                throws Exception {
        }
        private void print(String method){
            StringBuffer buf = new StringBuffer();
            if(pool != null){
                buf.append("method = " + method);
                buf.append(" getMaxIdle = " + pool.getMaxIdle());
                buf.append(" getMinIdle = " + pool.getMinIdle());
                buf.append(" getNumActive = " + pool.getNumActive());
                buf.append(" getNumIdle = " + pool.getNumIdle());
                buf.append(" getNumWaiters = " + pool.getNumWaiters());
                buf.append(" getRemoveAbandonedTimeout = " + pool.getRemoveAbandonedTimeout());
                buf.append(" getBorrowedCount = " + pool.getBorrowedCount());
                buf.append(" getCreatedCount = " + pool.getCreatedCount());
                buf.append(" getDestroyedByBorrowValidationCount = " + pool.getDestroyedByBorrowValidationCount());
                buf.append(" getDestroyedByEvictorCount = " + pool.getDestroyedByEvictorCount());
                buf.append(" getDestroyedCount = " + pool.getDestroyedCount());
                buf.append(" getMaxBorrowWaitTimeMillis = " + pool.getMaxBorrowWaitTimeMillis());
                buf.append(" getMaxTotal = " + pool.getMaxTotal());
                buf.append(" getMaxWaitMillis = " + pool.getMaxWaitMillis());
                buf.append(" getMinEvictableIdleTimeMillis = " + pool.getMinEvictableIdleTimeMillis());
                buf.append(" getReturnedCount = " + pool.getReturnedCount());
                buf.append(" getTimeBetweenEvictionRunsMillis = " + pool.getTimeBetweenEvictionRunsMillis());
            }
            System.out.println(buf.toString());
        }
    }
}
