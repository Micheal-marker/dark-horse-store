package com.hmall.item.service;

import com.hmall.api.dto.ItemDTO;
import com.hmall.common.domain.PageDTO;
import com.hmall.item.domain.query.ItemPageQuery;

import java.io.IOException;

public interface IEsItemService {
    /**
     * 基于ES搜索商品
     */
    PageDTO<ItemDTO> search(ItemPageQuery query) throws IOException;
}
