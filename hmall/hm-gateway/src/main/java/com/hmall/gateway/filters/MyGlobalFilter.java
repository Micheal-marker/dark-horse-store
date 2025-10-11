package com.hmall.gateway.filters;

import org.springframework.cloud.gateway.filter.GatewayFilterChain;
import org.springframework.cloud.gateway.filter.GlobalFilter;
import org.springframework.core.Ordered;
import org.springframework.http.HttpHeaders;
import org.springframework.http.server.reactive.ServerHttpRequest;
import org.springframework.stereotype.Component;
import org.springframework.web.server.ServerWebExchange;
import reactor.core.publisher.Mono;

@Component
public class MyGlobalFilter implements GlobalFilter , Ordered {

    public Mono<Void> filter(ServerWebExchange exchange, GatewayFilterChain chain) {
        // 获取请求头
        ServerHttpRequest request = exchange.getRequest();
        HttpHeaders headers = request.getHeaders();

        // 校验token
//        System.out.println("MyGlobalFilter: " + headers);

        // 放行
        return chain.filter(exchange);
    }

    public int getOrder() {
        // 过滤器的顺序，数值越小，优先级越高
        return 0;
    }
}
