package redis.clients.jedis;

import redis.clients.util.Hashing;

import java.util.HashSet;
import java.util.List;
import java.util.Random;
import java.util.Set;
import java.util.regex.Pattern;


public class VshardedJedis extends ShardedJedis {


    public VshardedJedis(List<JedisShardInfo> shards) {
        super(shards);
    }

    public VshardedJedis(List<JedisShardInfo> shards, Hashing algo) {
        super(shards, algo);
    }

    public VshardedJedis(List<JedisShardInfo> shards, Pattern keyTagPattern) {
        super(shards, keyTagPattern);
    }

    public VshardedJedis(List<JedisShardInfo> shards, Hashing algo, Pattern keyTagPattern) {
        super(shards, algo, keyTagPattern);
    }

    /**
     * get redis keys
     * @param pattern
     * @return All keys
     */
    public Set<String> keys(final String pattern) {
        Set<String> result = new HashSet<String>();
        for(Jedis j:this.getAllShards()){
            result.addAll(j.keys(pattern));
        }
        return result;
    }

    /**
     * Delete redis's key
     * @param key
     * @return deleted key's count.
     */
    @Override
    public Long del(String key) {
        Long count = 0L;
        for(Jedis j:this.getAllShards()){
            count +=j.del(key);
        }
        return count;
    }

    /**
     * Delete redis's keys
     * @param key
     * @return
     */
    public Long del(String... key) {
        Long count = 0L;
        for(Jedis j:this.getAllShards()){
            count +=j.del(key);
        }
        return count;
    }

    /**
     *
     * @param key
     * @param members
     * @return
     */
    @Override
    public Long sadd(String key, String... members) {
        Long count = 0L;
        for(String member:members){
            Jedis j = getShard(member);
            count += j.sadd(key,member);
        }
        return count;
    }

    /**
     *
     * @param key
     * @return
     */
    @Override
    public Set<String> smembers(String key) {
        Set<String> result = new HashSet<String>();
        for(Jedis j:this.getAllShards()){
            result.addAll(j.smembers(key));
        }

        return result;
    }

    /**
     *
     * @param key
     * @param members
     * @return
     */
    @Override
    public Long srem(String key, String... members) {
        Long count = 0L;
        for(String member:members){
            Jedis j = getShard(member);
            count += j.srem(key, members);
        }

        return count;
    }

    /**
     *
     * @param key
     * @return
     */
    @Override
    public String spop(String key) {
        String result = "";
        for(Jedis j:this.getAllShards()){
            result = j.spop(key);
            if(result != null){
                return result;
            }
        }
        return result;
    }
    /**
     *
     * @param key
     * @return
     */
    @Override
    public Long scard(String key) {
        Long count = 0L;
        for(Jedis j:this.getAllShards()){
            count += j.scard(key);
        }

        return count;
    }
    /**
     *
     * @param key
     * @param member
     * @return
     */
    @Override
    public Boolean sismember(String key, String member) {
        Jedis j = getShard(member);
        return j.sismember(key, member);
    }

    /**
     *
     * @param key
     * @return
     */
    @Override
    public String srandmember(String key) {
        Random random  = new Random();
        int inx = random.nextInt(this.getAllShards().size());
        Jedis j = this.getShard(inx + "-random");

        return j.srandmember(key);
    }

    /**
     *
     * @param key
     * @param count
     * @return
     */
    public List<String> srandmember(final String key, final int count) {
        Random random  = new Random();
        int inx = random.nextInt(this.getAllShards().size());
        Jedis j = this.getShard(inx + "-random");

        return j.srandmember(key, count);
    }
    /**
     * Move the specifided member from the set at srckey to the set at dstkey.
     * This operation is atomic, in every given moment the element will appear
     * to be in the source or destination set for accessing clients.
     * <p>
     * If the source set does not exist or does not contain the specified
     * element no operation is performed and zero is returned, otherwise the
     * element is removed from the source set and added to the destination set.
     * On success one is returned, even if the element was already present in
     * the destination set.
     * <p>
     * An error is raised if the source or destination keys contain a non Set
     * value.
     * <p>
     * Time complexity O(1)
     *
     * @param srckey
     * @param dstkey
     * @param member
     * @return Integer reply, specifically: 1 if the element was moved 0 if the
     *         element was not found on the first set and no operation was
     *         performed
     */
    public Long smove(final String srckey, final String dstkey,
                      final String member) {
        Long result = 0l;
        for(Jedis j:this.getAllShards()){
            result = result | j.smove(srckey, dstkey, member);
        }

        return result;
    }

    /**
     * Return the members of a set resulting from the intersection of all the
     * sets hold at the specified keys. Like in
     * {@link #lrange(String, long, long) LRANGE} the result is sent to the
     * client as a multi-bulk reply (see the protocol specification for more
     * information). If just a single key is specified, then this command
     * produces the same result as {@link #smembers(String) SMEMBERS}. Actually
     * SMEMBERS is just syntax sugar for SINTER.
     * <p>
     * Non existing keys are considered like empty sets, so if one of the keys
     * is missing an empty set is returned (since the intersection with an empty
     * set always is an empty set).
     * <p>
     * Time complexity O(N*M) worst case where N is the cardinality of the
     * smallest set and M the number of sets
     *
     * @param keys
     * @return Multi bulk reply, specifically the list of common elements.
     */
    public Set<String> sinter(final String... keys) {

        Set<String> result = new HashSet<String>();

        for(Jedis j:this.getAllShards()){
            result.addAll(j.sinter(keys));
        }

        return result;
    }

    /**
     * This commnad works exactly like {@link #sinter(String...) SINTER} but
     * instead of being returned the resulting set is sotred as dstkey.
     * <p>
     * Time complexity O(N*M) worst case where N is the cardinality of the
     * smallest set and M the number of sets
     *
     * @param dstkey
     * @param keys
     * @return Status code reply
     */
    public Long sinterstore(final String dstkey, final String... keys) {

        Long result = 0l;

        for(Jedis j:this.getAllShards()){
            result += j.sinterstore(dstkey, keys);
        }

        return result;
    }

    /**
     * Return the members of a set resulting from the union of all the sets hold
     * at the specified keys. Like in {@link #lrange(String, long, long) LRANGE}
     * the result is sent to the client as a multi-bulk reply (see the protocol
     * specification for more information). If just a single key is specified,
     * then this command produces the same result as {@link #smembers(String)
     * SMEMBERS}.
     * <p>
     * Non existing keys are considered like empty sets.
     * <p>
     * Time complexity O(N) where N is the total number of elements in all the
     * provided sets
     *
     * @param keys
     * @return Multi bulk reply, specifically the list of common elements.
     */
    public Set<String> sunion(final String... keys) {

        Set<String> result = new HashSet<String>();

        for(Jedis j:this.getAllShards()){
            result.addAll(j.sunion(keys));
        }

        return result;
    }

    /**
     * This command works exactly like {@link #sunion(String...) SUNION} but
     * instead of being returned the resulting set is stored as dstkey. Any
     * existing value in dstkey will be over-written.
     * <p>
     * Time complexity O(N) where N is the total number of elements in all the
     * provided sets
     *
     * @param dstkey
     * @param keys
     * @return Status code reply
     */
    public Long sunionstore(final String dstkey, final String... keys) {
        Long result = 0l;

        for(Jedis j:this.getAllShards()){
            result += j.sunionstore(dstkey, keys);
        }

        return result;
    }

    /**
     * Return the difference between the Set stored at key1 and all the Sets
     * key2, ..., keyN
     * <p>
     * <b>Example:</b>
     *
     * <pre>
     * key1 = [x, a, b, c]
     * key2 = [c]
     * key3 = [a, d]
     * SDIFF key1,key2,key3 => [x, b]
     * </pre>
     *
     * Non existing keys are considered like empty sets.
     * <p>
     * <b>Time complexity:</b>
     * <p>
     * O(N) with N being the total number of elements of all the sets
     *
     * @param keys
     * @return Return the members of a set resulting from the difference between
     *         the first set provided and all the successive sets.
     */
    public Set<String> sdiff(final String... keys) {

        Set<String> result = new HashSet<String>();

        for(Jedis j:this.getAllShards()){
            result.addAll(j.sdiff(keys));
        }

        return result;
    }

    /**
     * This command works exactly like {@link #sdiff(String...) SDIFF} but
     * instead of being returned the resulting set is stored in dstkey.
     *
     * @param dstkey
     * @param keys
     * @return Status code reply
     */
    public Long sdiffstore(final String dstkey, final String... keys) {
        Long result = 0l;

        for(Jedis j:this.getAllShards()){
            result += j.sdiffstore(dstkey, keys);
        }
        return result;
    }
    /**
     *
     * @param oldkey
     * @param newkey
     * @return
     */
    public String rename(final String oldkey, final String newkey) {
        String result = "";
        for(Jedis j:this.getAllShards()){
            result = j.rename(oldkey,newkey);
        }
        return result;
    }
    @Override
    public VshardedJedisPipeline pipelined() {
        VshardedJedisPipeline pipeline = new VshardedJedisPipeline();
        pipeline.setShardedJedis(this);
        return pipeline;
    }
}
