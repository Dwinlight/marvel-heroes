package repository;

import com.fasterxml.jackson.databind.JsonNode;
import env.ElasticConfiguration;
import env.MarvelHeroesConfiguration;
import models.PaginatedResults;
import models.SearchedHero;
import play.libs.Json;
import play.libs.ws.WSClient;
import utils.SearchedHeroSamples;

import javax.inject.Inject;
import javax.inject.Singleton;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;

@Singleton
public class ElasticRepository {

    private final WSClient wsClient;
    private final ElasticConfiguration elasticConfiguration;

    @Inject
    public ElasticRepository(WSClient wsClient, MarvelHeroesConfiguration configuration) {
        this.wsClient = wsClient;
        this.elasticConfiguration = configuration.elasticConfiguration;
    }


    public CompletionStage<PaginatedResults<SearchedHero>> searchHeroes(String input, int size, int page) {
        String in = (input == "" ? "*": input.trim());
        //return CompletableFuture.completedFuture(new PaginatedResults<>(3, 1, 1, Arrays.asList(SearchedHeroSamples.IronMan(), SearchedHeroSamples.MsMarvel(), SearchedHeroSamples.SpiderMan())));



        return wsClient.url(elasticConfiguration.uri +"/heroes/_search")
                 .post(Json.parse("{\"query\": {\"query_string\": {\"query\": \""+in+"\",\"fields\": [\"name^3\", \"secretIdentities^2\", \"secretIdentities^2\", \"aliases^2\"  ]} }, \"size\": "+size +", \"from\":"+size*(page-1)+"}"))
                 .thenApply(response -> {
                     JsonNode json = Json.parse(response.getBody()).get("hits").get("hits");
                     System.out.println(json);
                     List<SearchedHero> heros = new ArrayList<>();
                     json.forEach(h -> {
                            System.out.println(h.get("_source"));
                                 heros.add(SearchedHero.fromJson(h.get("_source")));

                             }
                             );
                    System.out.println(heros.size());
                     return new PaginatedResults<>(heros.size(),page,(heros.size()/size)+1,heros);
                });
    }

    public CompletionStage<List<SearchedHero>> suggest(String input) {
        return CompletableFuture.completedFuture(Arrays.asList(SearchedHeroSamples.IronMan(), SearchedHeroSamples.MsMarvel(), SearchedHeroSamples.SpiderMan()));
        // TODO
        // return wsClient.url(elasticConfiguration.uri + "...")
        //         .post(Json.parse("{ ... }"))
        //         .thenApply(response -> {
        //             return ...
        //         });
    }
}
