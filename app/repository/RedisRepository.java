package repository;
import com.sun.jdi.LongValue;

import io.lettuce.core.RedisClient;
import io.lettuce.core.RedisFuture;
import io.lettuce.core.api.StatefulRedisConnection;
import io.lettuce.core.api.async.RedisAsyncCommands;
import io.lettuce.core.api.sync.RedisCommands;
import models.StatItem;
import models.TopStatItem;
import play.Logger;

import utils.StatItemSamples;

import javax.inject.Inject;
import javax.inject.Singleton;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.stream.Collectors;

@Singleton
public class RedisRepository {

    private static Logger.ALogger logger = Logger.of("RedisRepository");


    private final RedisClient redisClient ;



    @Inject
    public RedisRepository(RedisClient redisClient) {
        this.redisClient = redisClient;
    }


    public CompletionStage<Boolean> addNewHeroVisited(StatItem statItem) {
        logger.info("hero visited " + statItem.name);

        return addHeroAsLastVisited(statItem).thenCombine(incrHeroInTops(statItem), (aLong, aBoolean) -> {
            return aBoolean && aLong > 0;
        });
    }

    private CompletionStage<Boolean> incrHeroInTops(StatItem statItem) {
        // TODO

        StatefulRedisConnection<String,String> connection = redisClient.connect();
        return  connection.async().zincrby("hits",1,statItem.toJson().toString()).thenApply(res -> {
            connection.close(); return true ;
        });

    }


    private CompletionStage<Long> addHeroAsLastVisited(StatItem statItem) {
        // TODO
        StatefulRedisConnection<String,String> connection = redisClient.connect();
        return connection.async().lpush("lasts",statItem.toJson().toString()).thenApply(res -> {
            connection.close();
            System.out.println("addHeroAsLastVisited " + res);
            return 1L;
        });
      /*  RedisCommands<String, String> syncCommands = redisClient.connect().sync();
        syncCommands.lpush("lasts",statItem.toJson().toString());
        return CompletableFuture.completedFuture(1L);
        */

    }

    public CompletionStage<List<StatItem>> lastHeroesVisited(int count) {
        logger.info("Retrieved last heroes");

        // TODO
        /*RedisCommands<String, String> syncCommands = redisClient.connect().sync();
        List<String> lasts5 = syncCommands.lrange("lasts",0,count-1);
        List<StatItem> lastsHeroes = new ArrayList<StatItem>() ;
        for ( int i = 0 ; i < lasts5.size(); i ++ ) {
            lastsHeroes.add(StatItem.fromJson(lasts5.get(i))) ;
        }
        // List<StatItem> lastsHeroes = Arrays.asList(StatItemSamples.IronMan(), StatItemSamples.Thor(), StatItemSamples.CaptainAmerica(), StatItemSamples.BlackWidow(), StatItemSamples.MsMarvel());
        return CompletableFuture.completedFuture(lastsHeroes);
        */

        // System.out.println(count);
        StatefulRedisConnection<String, String> connection = redisClient.connect();
        return connection.async().lrange("lasts",0,count-1).thenApply((res -> {

            System.out.println("lastHeroesVisited " + res.size());
            return res.stream().map(last -> StatItem.fromJson(last)).collect(Collectors.toList());
        }));
    }

    public CompletionStage<List<TopStatItem>> topHeroesVisited(int count) {
        StatefulRedisConnection<String, String> connection = redisClient.connect();
        logger.info("Retrieved tops heroes");
        return connection.async().zrevrangeWithScores("hits",0,count-1).thenApply(res -> {
            connection.close();
            return  res.stream().map(hit -> new TopStatItem(StatItem.fromJson(hit.getValue()),(long)hit.getScore())).collect(Collectors.toList()) ;

        }) ;
    }
}


