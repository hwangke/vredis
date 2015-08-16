import redis.clients.jedis.JedisPoolConfig;
import redis.clients.jedis.JedisShardInfo;
import redis.clients.jedis.VshardedJedisPipeline;
import redis.clients.jedis.VshardedJedisPool;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Set;


public class TestVshardedJedis {

    private static JedisPoolConfig jedisPoolConfig = new JedisPoolConfig();
    private static VshardedJedisPool pool;

    public static  void clean(){
        Set<String> set = pool.getResource().keys("*");
        for(Iterator it = set.iterator();it.hasNext();){
            String key = (String)it.next();
            System.out.print(" value = [" + key + "],");
            pool.getResource().del(key);
        }
    }
    public static void testSadd(){
        for(int i=0;i<10;i++){
            pool.getResource().sadd("vs-"+i,"hello"+i,"Value1_"+i,"Value2_"+i,"Value3_"+i);
        }
    }
    public static void testPipelineSadd(){
        System.out.println("================pipeline===================");
        VshardedJedisPipeline pipeline =  pool.getResource().pipelined();
        for(int i=0;i<100;i++){
            pipeline.sadd("psadd-1","value_"+i);
        }
        pipeline.syncAndReturnAll();

        Set<String> set = pool.getResource().smembers("psadd-1");
        System.out.print("psadd-1 members-");
        for(Iterator it = set.iterator();it.hasNext();){
            System.out.print(" value = [" + it.next() + "],");
        }
        System.out.println();
    }

    public static  void testSmembers(){
        for(int i=0;i<10;i++){
            Set<String> set = pool.getResource().smembers("vs-"+i);
            System.out.print("Smembers-"+i);
            for(Iterator it = set.iterator();it.hasNext();){
                System.out.print(" value = [" + it.next() + "],");
            }
            System.out.println();
        }
    }
    public static  void testSCARD(){
        for(int i=0;i<10;i++){
            Long value = pool.getResource().scard("vs-"+i);
            System.out.println("SCARD-" + i + ":" + value);
        }
    }
    public static  void testSDIFF(){
        pool.getResource().sadd("df-1","hello","Value1_1","Value2_2","Value3_3");
        pool.getResource().sadd("df-2","hello","Value1_2","Value2_2","Value3_3");

        Set<String> df_mb = pool.getResource().smembers("df-1");
        System.out.print("df-1 members");
        for(Iterator it = df_mb.iterator();it.hasNext();){
            System.out.print(" value = [" + it.next() + "],");
        }
        System.out.println();

        Set<String> df2_mb = pool.getResource().smembers("df-2");
        System.out.print("df-2 members");
        for(Iterator it = df2_mb.iterator();it.hasNext();){
            System.out.print(" value = [" + it.next() + "],");
        }
        System.out.println();

        Set<String> set = pool.getResource().sdiff("df-1","df-2");

        System.out.println("SDIFF: df-1 & df-2:");
        for(Iterator it = set.iterator();it.hasNext();){
            System.out.print(" value = [" + it.next() + "],");
        }
        System.out.println();
    }
    public static  void testSDIFFSTORE(){
        for(int i=0;i<10;i++){
            Long value = pool.getResource().sdiffstore("df-1", "df-2");
            System.out.println("df-1 & df-2 SDIFFSTORE:"+value);
        }
    }
    public static  void testSINTER(){
        pool.getResource().sadd("inter-1","hello","Value1_1","Value2_2","Value3_3");
        pool.getResource().sadd("inter-2","hello","Value1_2","Value2_2","Value3_3");

        Set<String> in_mb = pool.getResource().smembers("inter-1");
        System.out.print("inter-1 members");
        for(Iterator it = in_mb.iterator();it.hasNext();){
            System.out.print(" value = [" + it.next() + "],");
        }
        System.out.println();

        Set<String> in2_mb = pool.getResource().smembers("inter-2");
        System.out.print("inter-2 members");
        for(Iterator it = in2_mb.iterator();it.hasNext();){
            System.out.print(" value = [" + it.next() + "],");
        }
        System.out.println();

        Set<String> set = pool.getResource().sinter("inter-1","inter-2");

        System.out.println("SINTER inter-1 & inter-2:");
        for(Iterator it = set.iterator();it.hasNext();){
            System.out.print(" value = [" + it.next() + "],");
        }

    }
    public static  void testSINTERSTORE(){
        Long value = pool.getResource().sinterstore("inter-1" ,"inter-2");
        System.out.println("SINTERSTORE inter-1 & inter-2 :"+value);

    }
    public static  void testSISMEMBER(){
        Boolean result = pool.getResource().sismember("inter-1","hello");
        System.out.println("SISMEMBER-"  + result);
        result = pool.getResource().sismember("inter-1","Value1_1");
        System.out.println("SISMEMBER-"  + result);

    }
    public static  void testSMOVE(){
        Long result = pool.getResource().smove("inter-1","inter-2", "Value1_1");
        System.out.println("SMOVE-"+result);
        Boolean ismember = pool.getResource().sismember("inter-1", "Value1_1");
        System.out.println("false SISMEMBER-"  + result);
        ismember = pool.getResource().sismember("inter-2","Value1_1");
        System.out.println("true SISMEMBER-"  + result);

    }

    public static  void testSPOP(){
        String result = pool.getResource().spop("inter-2");
        System.out.println("SPOP-"+result);
    }
    public static  void testSRANDMEMBER(){
        String result = pool.getResource().srandmember("inter-2");
        System.out.println("SRANDMEMBER-"+result);
    }
    public static  void testSREM(){
        Long  result = pool.getResource().srem("inter-2" , "Value3_3");
        System.out.println("srem-"+result);
    }
    public static  void testSUNION(){
        Set<String> set = pool.getResource().sunion("inter-1" ,"inter-2");
        System.out.print("SUNION-");
        for(Iterator it = set.iterator();it.hasNext();){
            System.out.print(" value = [" + it.next() + "],");
        }
        System.out.println();
    }
    public static  void testSUNIONSTORE(){
        Long  result = pool.getResource().sunionstore("inter-1" ,"inter-2");
        System.out.println("SUNIONSTORE:"+result);
    }

    public static void main(String[] args) {
        jedisPoolConfig.setMaxIdle(10);
        jedisPoolConfig.setMaxWaitMillis(1000 * 10);
        jedisPoolConfig.setMaxTotal(100*1000);
        jedisPoolConfig.setTestOnBorrow(true);

        List<JedisShardInfo> list = new ArrayList<JedisShardInfo>();
        JedisShardInfo s1 = new JedisShardInfo("192.168.42.11",6379);
        JedisShardInfo s2 = new JedisShardInfo("192.168.42.29",6379);
        list.add(s1);
        list.add(s2);

        //JedisShardInfo
        pool = new VshardedJedisPool(jedisPoolConfig,list,null);

        clean();
        testSadd();
        testSmembers();
        testSCARD();
        testSDIFF();
        testSDIFFSTORE();
        testSINTER();
        testSINTERSTORE();
        testSISMEMBER();
        testSMOVE();
        testSPOP();
        testSRANDMEMBER();
        testSREM();
        testSUNION();
        testSUNIONSTORE();
        testPipelineSadd();
    }
}
