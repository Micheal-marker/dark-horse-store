package com.hmall.trade.constants;

public interface MQConstants {

    // 延迟交换机名称
    String DELAY_EXCHANGE_NAME = "trade.delay.direct";

    // 延迟队列名称
    String DELAY_ORDER_QUEUE_NAME = "trade.delay.order.queue";

    // 延迟路由键
    String DELAY_ORDER_KEY = "delay.order";
}
