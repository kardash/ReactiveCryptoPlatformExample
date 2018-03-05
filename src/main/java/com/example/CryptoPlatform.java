package com.example;

import com.example.external.CryptoConnectionHolder;
import com.example.service.PriceService;
import com.example.service.TradeService;
import com.example.utils.JsonUtils;
import com.example.utils.LoggerConfigurationTrait;
import com.example.utils.NettyUtils;
import org.reactivestreams.Publisher;
import reactor.core.publisher.Flux;
import reactor.ipc.netty.http.server.HttpServer;
import reactor.ipc.netty.http.websocket.WebsocketInbound;
import reactor.ipc.netty.http.websocket.WebsocketOutbound;

import java.util.Optional;
import java.util.function.BiFunction;
import java.util.logging.Logger;

import static com.example.utils.HttpResourceResolver.resourcePath;

public class CryptoPlatform extends LoggerConfigurationTrait {
    private static final Logger logger = Logger.getLogger("http-server");

    public static void main(String[] args) {
        CryptoConnectionHolder externalSystemDataStream = new CryptoConnectionHolder();
        PriceService priceService = new PriceService();
        TradeService tradeService = new TradeService();

        HttpServer.create(8080)
                .startRouterAndAwait(hsr -> hsr
                        .ws(
                                "/stream",
                                handleWebsocket(externalSystemDataStream, priceService, tradeService)
                        )
                        .file("/favicon.ico", resourcePath("ui/favicon.ico"))
                        .file("/main.js", resourcePath("ui/main.js"))
                        .file("/**", resourcePath("ui/index.html"))
                );
    }

    private static BiFunction<WebsocketInbound, WebsocketOutbound, Publisher<Void>> handleWebsocket(
            CryptoConnectionHolder externalSystemDataStream,
            PriceService priceService,
            TradeService tradeService
    ) {
        return (req, res) ->
                externalSystemDataStream.listenForExternalEvents()
                        .transform(tradingDataStream -> {
                            Flux<Long> priceAverageIntervalCommands = NettyUtils
                                    .prepareInput(req)
                                    .doOnNext(inMessage -> logger.info("[WS] >> " + inMessage))
                                    .transform(CryptoPlatform::handleRequestedAveragePriceIntervalValue);

                            return Flux.merge(
                                    priceService.calculatePriceAndAveragePrice(
                                            tradingDataStream,
                                            priceAverageIntervalCommands

                                    ),
                                    tradeService.tradingEvents(
                                            tradingDataStream
                                    ));
                        })
                        .map(JsonUtils::writeAsString)
                        .doOnNext(outMessage -> logger.info("[WS] << " + outMessage))
                        .transform(CryptoPlatform::handleOutgoingStreamBackpressure)
                        .transform(NettyUtils.prepareOutbound(res));
    }

    // Visible for testing
    public static Flux<Long> handleRequestedAveragePriceIntervalValue(Flux<String> requestedInterval) {
        // TODO: input may be incorrect, pass only correct interval
        // TODO: ignore invalid values (empty, non number, <= 0, > 60)
//        return Flux.never();

        return requestedInterval
                .map(req -> {
                    try {
                        return Optional.of(Long.parseLong(req));
                    } catch (Exception e) {
                        logger.info("Requested invalid average price interval: " + req);
                        return Optional.<Long>empty();
                    }
                })
                .filter(Optional::isPresent)
                .map(Optional::get)
                .filter(req -> req > 0 && req < 60);
    }

    // Visible for testing
    public static Flux<String> handleOutgoingStreamBackpressure(Flux<String> outgoingStream) {
        // TODO: Add backpressure handling
        // It is possible that writing data to output may be slower than rate of
        // incoming output data
//        return outgoingStream;

        return outgoingStream
                .onBackpressureBuffer(10);
    }


}
