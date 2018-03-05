package com.example.service;

import com.example.dto.MessageDTO;
import com.example.service.utils.MessageMapper;
import reactor.core.publisher.Flux;

import java.util.Map;
import java.util.logging.Logger;

public class TradeService {
    private static final Logger logger = Logger.getLogger("trade-service");

    public Flux<MessageDTO> tradingEvents(
            Flux<Map<String, Object>> input
    ) {
        // TODO: Add implementation to produce trading events
//        return Flux.never();

        return input
                .doOnNext(event -> logger.fine("[TRADING-SYSTEM] >> " + event))
                .filter(MessageMapper::isTradeMessageType)
                .map(MessageMapper::mapToTradeMessage);
    }

}
