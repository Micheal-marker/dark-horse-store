package com.hmall.item.controller;


import cn.hutool.core.util.StrUtil;
import com.baomidou.mybatisplus.extension.plugins.pagination.Page;
import com.hmall.api.dto.ItemDTO;
import com.hmall.common.domain.PageDTO;
import com.hmall.item.domain.po.Item;
import com.hmall.item.domain.query.ItemPageQuery;
import com.hmall.item.service.IEsItemService;
import com.hmall.item.service.IItemService;
import io.swagger.annotations.Api;
import io.swagger.annotations.ApiOperation;
import lombok.RequiredArgsConstructor;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.io.IOException;

@Api(tags = "搜索相关接口")
@RestController
@RequestMapping("/search")
@RequiredArgsConstructor
public class SearchController {

//    private final IItemService itemService;
    private final IEsItemService esItemService;

//    /**
//     * 基于数据库查询商品
//     * @param query
//     * @return
//     */
//    @ApiOperation("搜索商品")
//    @GetMapping("/list")
//    public PageDTO<ItemDTO> search(ItemPageQuery query) {
//        // 分页查询
//        Page<Item> result = itemService.lambdaQuery()
//                .like(StrUtil.isNotBlank(query.getKey()), Item::getName, query.getKey())
//                .eq(StrUtil.isNotBlank(query.getBrand()), Item::getBrand, query.getBrand())
//                .eq(StrUtil.isNotBlank(query.getCategory()), Item::getCategory, query.getCategory())
//                .eq(Item::getStatus, 1)
//                .between(query.getMaxPrice() != null, Item::getPrice, query.getMinPrice(), query.getMaxPrice())
//                .page(query.toMpPage("update_time", false));
//        // 封装并返回
//        return PageDTO.of(result, ItemDTO.class);
//    }

    /**
     * 基于ES搜索商品
     * @param query
     * @return
     * @throws IOException
     */
    @ApiOperation("搜索商品（基于ES）")
    @GetMapping("/list")
    public PageDTO<ItemDTO> search(ItemPageQuery query) throws IOException {
        return esItemService.search(query);
    }
}
