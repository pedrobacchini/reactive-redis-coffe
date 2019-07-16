package com.github.pedrobacchini.reactiverediscoffe.config;

import com.github.pedrobacchini.reactiverediscoffe.domain.Coffee;
import org.springframework.data.redis.connection.ReactiveRedisConnectionFactory;
import org.springframework.data.redis.core.ReactiveRedisOperations;
import org.springframework.stereotype.Component;
import reactor.core.publisher.Flux;

import javax.annotation.PostConstruct;
import java.util.UUID;

@Component
public class CoffeLoader {

    private final ReactiveRedisConnectionFactory factory;
    private final ReactiveRedisOperations<String, Coffee> coffeeOps;

    public CoffeLoader(ReactiveRedisConnectionFactory factory,
                       ReactiveRedisOperations<String, Coffee> coffeeOps) {
        this.factory = factory;
        this.coffeeOps = coffeeOps;
    }

    @PostConstruct
    public void loadData() {
        factory.getReactiveConnection().serverCommands().flushAll()
                .thenMany(
                        Flux.just("Jet Black Redis", "Darth Redis", "Black Alert Redis")
                                .map(coffeeName -> new Coffee(UUID.randomUUID().toString(), coffeeName))
                                .flatMap(coffee -> coffeeOps.opsForValue().set(coffee.getId(), coffee))
                )
                .thenMany(coffeeOps.keys("*"))
                .flatMap(coffeeOps.opsForValue()::get)
                .subscribe(System.out::println);
    }
}
