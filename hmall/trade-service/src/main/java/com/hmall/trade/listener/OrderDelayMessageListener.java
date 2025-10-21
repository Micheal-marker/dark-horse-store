package com.hmall.trade.listener;

import com.hmall.api.client.PayClient;
import com.hmall.api.dto.PayOrderDTO;
import com.hmall.trade.constants.MQConstants;
import com.hmall.trade.domain.po.Order;
import com.hmall.trade.service.IOrderService;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.amqp.rabbit.annotation.Exchange;
import org.springframework.amqp.rabbit.annotation.Queue;
import org.springframework.amqp.rabbit.annotation.QueueBinding;
import org.springframework.amqp.rabbit.annotation.RabbitListener;
import org.springframework.stereotype.Component;

@Component
@RequiredArgsConstructor
@Slf4j
public class OrderDelayMessageListener {

    private final IOrderService orderService;
    private final PayClient payClient;


    @RabbitListener(bindings = @QueueBinding(
            value = @Queue(name = MQConstants.DELAY_ORDER_QUEUE_NAME, durable = "true"),
            exchange = @Exchange(name = MQConstants.DELAY_EXCHANGE_NAME, delayed = "true"),
            key = MQConstants.DELAY_ORDER_KEY
    ))
    public void listenOrderDelayMessage(Long orderId){
        // 1. 查询订单
        Order order = orderService.getById(orderId);

        // 2. 判断订单状态，如果不是未支付，关闭订单
        if(order != null && order.getStatus() != 1){
            return;
        }

        // 3. 未支付，查询订单流水状态（远程调用）
        PayOrderDTO payOrder = payClient.queryPayOrderByBizOrderNo(orderId);

        // 4. 判断订单状态
        if(payOrder != null && payOrder.getStatus() == 3){
            // 4.1 如果支付成功，更新订单状态为已支付
            orderService.markOrderPaySuccess(orderId);
        } else {
            // 4.2 如果支付失败，取消订单，回滚库存
            log.info("订单超时未支付，取消订单，订单ID：{}", orderId);
            orderService.cancelOrder(orderId);
        }
    }
}
