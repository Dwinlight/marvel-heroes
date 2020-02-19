package repository;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.mongodb.reactivestreams.client.MongoCollection;
import com.mongodb.reactivestreams.client.MongoDatabase;
import models.Hero;
import models.ItemCount;
import models.YearAndUniverseStat;
import org.bson.Document;
import play.libs.Json;
import utils.HeroSamples;
import utils.ReactiveStreamsUtils;

import javax.inject.Inject;
import javax.inject.Singleton;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

@Singleton
public class MongoDBRepository {

    private final MongoCollection<Document> heroesCollection;

    @Inject
    public MongoDBRepository(MongoDatabase mongoDatabase) {
        this.heroesCollection = mongoDatabase.getCollection("heroes");
    }


    public CompletionStage<Optional<Hero>> heroById(String heroId) {
        //return HeroSamples.staticHero(heroId);

        //String query = "{id:"+ heroId +"}";
        //Document document = Document.parse(query);
        Document document = new Document("id",heroId);
        return ReactiveStreamsUtils.fromSinglePublisher(heroesCollection.find(document).first())
                 .thenApply(result -> Optional.ofNullable(result).map(Document::toJson).map(Hero::fromJson));
    }

    public CompletionStage<List<YearAndUniverseStat>> countByYearAndUniverse() {
        List<Document> pipeline = new ArrayList<>();

        String match = "{ $match: {\"identity.yearAppearance\": {$ne:\"\"}}}";


        String sort1 = "{ $group:{ _id:{yearAppearance:"+"\"$identity.yearAppearance\""+", universe:"+"\"$identity.universe\""+"}, count: {"+"\"$sum\""+":1} }}";
        String sort2 = "{$group:{_id:{yearAppearance:"+"\"$_id.yearAppearance\""+"},byUniverse:{"+"\"$push\""+":{universe:"+"\"$_id.universe\""+",count: "+"\"$count\""+"}}}}";
        Document document0 = Document.parse(match);
        Document document1 = Document.parse(sort1);
        Document document2 = Document.parse(sort2);
        pipeline.add(document0);
        pipeline.add(document1);
        pipeline.add(document2);
        return ReactiveStreamsUtils.fromMultiPublisher(heroesCollection.aggregate(pipeline))
                .thenApply(documents -> {
                    return documents.stream()
                                    .map(Document::toJson)
                                    .map(Json::parse)
                                    .map(jsonNode -> {
                                        int year = jsonNode.findPath("_id").findPath("yearAppearance").asInt();
                                        ArrayNode byUniverseNode = (ArrayNode) jsonNode.findPath("byUniverse");
                                        Iterator<JsonNode> elements = byUniverseNode.elements();
                                        Iterable<JsonNode> iterable = () -> elements;
                                        List<ItemCount> byUniverse = StreamSupport.stream(iterable.spliterator(), false)
                                                .map(node -> new ItemCount(node.findPath("universe").asText(), node.findPath("count").asInt()))
                                                .collect(Collectors.toList());
                                        return new YearAndUniverseStat(year, byUniverse);

                                    })
                                    .collect(Collectors.toList());
                });
    }


    public CompletionStage<List<ItemCount>> topPowers(int top) {

        List<Document> pipeline = new ArrayList<>();
        String unwind = "{ $unwind:"+"\"$powers\""+"}";
        String match = "{ $match: {\"powers\": {$ne:\"\"}}}";
        //String group = "{$group:{_id:{power:"+"\"$powers\""+" },count :{"+"\"$sum\""+":1}}}";
        String group = "{$group:{_id:"+"\"$powers\""+",count :{"+"\"$sum\""+":1}}}";
        String sort = "{"+"\"$sort\""+":{"+"\"count\""+":-1}}";
        String limit = "{ $limit : "+top+" }";
        Document matchDoc = Document.parse(match);
        Document unwindDoc = Document.parse(unwind);
        Document groupDoc = Document.parse(group);
        Document sortDoc = Document.parse(sort);
        Document limitDoc = Document.parse(limit);

        pipeline.add(matchDoc);
        pipeline.add(unwindDoc);
        pipeline.add(groupDoc);
        pipeline.add(sortDoc);
        pipeline.add(limitDoc);
        return ReactiveStreamsUtils.fromMultiPublisher(heroesCollection.aggregate(pipeline))
                 .thenApply(documents -> {
                     return documents.stream()
                             .map(Document::toJson)
                             .map(Json::parse)
                             .map(jsonNode -> {
                                 System.out.println(jsonNode.findPath("_id").asText());
                                 System.out.println(jsonNode.findPath("count").asInt());

                                 return new ItemCount(jsonNode.findPath("_id").asText(), jsonNode.findPath("count").asInt());
                             })
                             .collect(Collectors.toList());
                 });
    }

    public CompletionStage<List<ItemCount>> byUniverse() {

        List<Document> pipeline = new ArrayList<>();
        String match = "{ $match: {\"universe\": {$ne:\"\"}}}";
        String group = "{$group:{_id:"+"\"$identity.universe\""+",count :{"+"\"$sum\""+":1}}}";
        String sort = "{"+"\"$sort\""+":{"+"\"count\""+":-1}}";
        Document matchDoc = Document.parse(match);
        Document groupDoc = Document.parse(group);
        Document sortDoc = Document.parse(sort);
        pipeline.add(matchDoc);
        pipeline.add(groupDoc);
        pipeline.add(sortDoc);
        return ReactiveStreamsUtils.fromMultiPublisher(heroesCollection.aggregate(pipeline))
                 .thenApply(documents -> {
                     return documents.stream()
                             .map(Document::toJson)
                             .map(Json::parse)
                             .map(jsonNode -> {
                                return new ItemCount(jsonNode.findPath("_id").asText(), jsonNode.findPath("count").asInt());
                             })
                            .collect(Collectors.toList());
               });
    }

}
