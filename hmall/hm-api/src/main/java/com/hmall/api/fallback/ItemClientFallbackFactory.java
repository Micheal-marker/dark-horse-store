package com.hmall.api.fallback;

import com.hmall.api.client.ItemClient;
import com.hmall.api.dto.ItemDTO;
import com.hmall.api.dto.OrderDetailDTO;
import com.hmall.common.utils.CollUtils;
import lombok.extern.slf4j.Slf4j;
import org.springframework.cloud.openfeign.FallbackFactory;

import java.util.Collection;
import java.util.List;

@Slf4j
public class ItemClientFallbackFactory implements FallbackFactory<ItemClient> {
    @Override
    public ItemClient create(Throwable cause) {
        return new ItemClient() {

            @Override
            public List<ItemDTO> queryItemByIds(Collection<Long> ids) {
                log.info("查询商品失败！", cause);
                return CollUtils.newArrayList();
            }

            @Override
            public void deductStock(Collection<OrderDetailDTO> items) {
                log.info("扣减商品库存失败！", cause);
                throw new RuntimeException(cause);
            }
        };
    }
}
