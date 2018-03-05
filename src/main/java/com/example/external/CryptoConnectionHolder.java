package com.example.external;

import com.example.external.utils.PriceMessageUnpacker;
import com.example.external.utils.TradeMessageUnpacker;
import reactor.core.publisher.Flux;

import java.util.Arrays;
import java.util.Map;

//TODO turn to multi-subscriber with processor or another similar operator
//TODO add small history for each subscriber
//TODO add resilience

public class CryptoConnectionHolder {
    public static final int CACHE_SIZE = 3;

    private final Flux<Map<String, Object>> reactiveCryptoListener;

    public CryptoConnectionHolder() {
        reactiveCryptoListener = ReactiveCryptoListener
                .connect(
                        Flux.just("5~CCCAGG~BTC~USD", "0~Poloniex~BTC~USD"),
                        Arrays.asList(new PriceMessageUnpacker(), new TradeMessageUnpacker())
                )
                .transform(CryptoConnectionHolder::provideResilience)
                .transform(CryptoConnectionHolder::provideCaching);
    }

    public Flux<Map<String, Object>> listenForExternalEvents() {
        return reactiveCryptoListener;
    }

    // TODO: implement resilience such as retry with delay
    public static <T> Flux<T> provideResilience(Flux<T> input) {
//        return input;

        return input
                .retry();
    }


    // TODO: implement caching of 3 last elements & multi subscribers support
    public static <T> Flux<T> provideCaching(Flux<T> input) {
//        return input;
        return input.replay(CACHE_SIZE)
                .autoConnect();
    }
}
