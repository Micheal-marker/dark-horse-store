package com.hmall.gateway.filters;

import cn.hutool.core.text.AntPathMatcher;
import com.hmall.common.exception.UnauthorizedException;
import com.hmall.gateway.config.AuthProperties;
import com.hmall.gateway.utils.JwtTool;
import lombok.RequiredArgsConstructor;
import org.springframework.cloud.gateway.filter.GatewayFilterChain;
import org.springframework.cloud.gateway.filter.GlobalFilter;
import org.springframework.core.Ordered;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpStatus;
import org.springframework.http.server.reactive.ServerHttpRequest;
import org.springframework.http.server.reactive.ServerHttpResponse;
import org.springframework.stereotype.Component;
import org.springframework.web.server.ServerWebExchange;
import reactor.core.publisher.Mono;

import java.util.List;

@Component
@RequiredArgsConstructor
public class AuthGlobalFilter implements GlobalFilter, Ordered {

    private final JwtTool jwtTool;

    private final AuthProperties authProperties;
    // 路径匹配器（支持通配符**、*）
    private final AntPathMatcher pathMatcher = new AntPathMatcher();

    public Mono<Void> filter(ServerWebExchange exchange, GatewayFilterChain chain) {
        // 1.获取request
        ServerHttpRequest request = exchange.getRequest();

        // 2.判断是否需要做登录拦截
        if(isExcluded(request.getPath().toString())) {
            return chain.filter(exchange);
        }

        // 3.获取token
        String token = null;
        HttpHeaders headers = request.getHeaders();
        List<String> authorizationHeaders  = headers.get("authorization");
        if(authorizationHeaders  != null && !authorizationHeaders .isEmpty()) {
            token = authorizationHeaders.get(0);
        }

        // 4.校验并解析token
        Long userId;
        try {
            userId = jwtTool.parseToken(token);
        } catch (UnauthorizedException e) {
            ServerHttpResponse response = exchange.getResponse();
            response.setStatusCode(HttpStatus.UNAUTHORIZED);
            return response.setComplete();
        }

        // 5.传递用户信息
        String userInfo = userId.toString();
        ServerWebExchange swe = exchange.mutate()
                .request(builder -> builder.header("user-info", userInfo))
                .build();

//        System.out.println("userInfo = " + userInfo);

        // 6.放行
        return chain.filter(swe);
    }

    private boolean isExcluded(String requestURI) {
        for (String pathPattern : authProperties.getExcludePaths()) {
            if (pathMatcher.match(pathPattern, requestURI)) {
                return true;
            }
        }
        return false;
    }

    public int getOrder() {
        return 0;
    }
}
