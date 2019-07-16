package com.github.pedrobacchini.reactiverediscoffe.resource;

import com.github.pedrobacchini.reactiverediscoffe.domain.Coffee;
//import io.lettuce.core.pubsub.api.reactive.PatternMessage;
import org.springframework.data.redis.connection.ReactiveSubscription;
import org.springframework.data.redis.core.ReactiveRedisOperations;
import org.springframework.data.redis.core.ReactiveRedisTemplate;
import org.springframework.http.MediaType;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RestController;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.publisher.ReplayProcessor;

import java.util.Map;
import java.util.UUID;

@RestController
public class CoffeController {

    private final ReactiveRedisOperations<String, Coffee> coffeeOps;

    private ReplayProcessor<Coffee> replayProcessor = ReplayProcessor.create(100);


    public CoffeController(ReactiveRedisOperations<String, Coffee> coffeeOps,
                           ReactiveRedisTemplate<String, String> redisTemplate) {
        this.coffeeOps = coffeeOps;

//        coffeeOps.listenToPattern("__key*__:*").subscribe(o -> {
//            System.out.println(o.toString());
//        });

        coffeeOps.keys("*").flatMap(coffeeOps.opsForValue()::get).subscribe(coffee -> replayProcessor.onNext(coffee));

        redisTemplate.listenToPattern("__keyevent*:*").subscribe(o -> {
            ReactiveSubscription.PatternMessage patternMessage = (ReactiveSubscription.PatternMessage) o;
            System.out.println(o);
            System.out.println(patternMessage);
            System.out.println(((ReactiveSubscription.PatternMessage) o).getMessage());

            Flux<Coffee> coffeeFlux = coffeeOps.keys(patternMessage.getMessage().toString())
                    .flatMap(coffeeOps.opsForValue()::get);

            Coffee coffee = coffeeFlux.blockFirst();
            System.out.println(coffee);

            replayProcessor.onNext(coffee);
        });
    }

//    @GetMapping(produces = MediaType.TEXT_EVENT_STREAM_VALUE)
//    public Flux<Coffee> all() {
//        return coffeeOps.keys("*")
//                .flatMap(coffeeOps.opsForValue()::get).subscribeWith(replayProcessor);
//    }

    @GetMapping(value = "/sse", produces = MediaType.TEXT_EVENT_STREAM_VALUE)
    public Flux<Coffee> allSEE() {
        return replayProcessor;
    }

    @PostMapping
    public Mono<Boolean> save(@RequestBody Coffee coffee) {
        coffee.setId(UUID.randomUUID().toString());
        return coffeeOps.opsForValue().set(coffee.getId(), coffee);
    }
}
