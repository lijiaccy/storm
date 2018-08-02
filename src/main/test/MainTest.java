/*
import redis.clients.jedis.Jedis;
import redis.clients.jedis.Pipeline;

public class MainTest {
    public static void main(String[] args) {
        String x="usernamehahahahahahahhahahahkdalkfaldjflakdjfladflkkdjflajfdakdsldkdslkdflsfls";
        System.out.println(x.length());

        Jedis jedis = new Jedis("172.30.154.123",6379);
        jedis.auth("123456");
        Pipeline pipelined = jedis.pipelined();
        long start = System.currentTimeMillis();
        for (int i = 0; i < 100000; i++) {
            pipelined.set("pho"+i,"user"+i);
        }
        pipelined.syncAndReturnAll();
        long emd = System.currentTimeMillis();
        System.out.println(emd -start);
        jedis.disconnect();
    }
}
*/
