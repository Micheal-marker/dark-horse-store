package com.hmall.item.service.impl;

import cn.hutool.json.JSONUtil;
import com.hmall.api.dto.ItemDTO;
import com.hmall.common.domain.PageDTO;
import com.hmall.item.domain.query.ItemPageQuery;
import com.hmall.item.service.IEsItemService;
import lombok.RequiredArgsConstructor;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.text.Text;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.index.query.RangeQueryBuilder;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.search.fetch.subphase.highlight.HighlightBuilder;
import org.elasticsearch.search.fetch.subphase.highlight.HighlightField;
import org.elasticsearch.search.sort.SortBuilders;
import org.elasticsearch.search.sort.SortOrder;
import org.springframework.stereotype.Service;

import javax.swing.*;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

@Service
@RequiredArgsConstructor
public class EsItemServiceImpl implements IEsItemService {

    private final RestHighLevelClient esClient;

    /**
     * 基于ES搜索商品
     * @param query
     * @return
     */
    public PageDTO<ItemDTO> search(ItemPageQuery query) throws IOException {
        // 1.创建request对象
        SearchRequest request = new SearchRequest("items");

        // 2.构建查询条件
        SearchSourceBuilder sourceBuilder = request.source();

        // 2.1.搜索条件过滤
        BoolQueryBuilder boolQuery = QueryBuilders.boolQuery();
        // 2.1.1.关键字搜索（商品名称）
        if(query.getKey() != null && !query.getKey().isEmpty()) {
            boolQuery.must(QueryBuilders.matchQuery("name", query.getKey()));

            // 设置高亮
            sourceBuilder.highlighter(
                    SearchSourceBuilder
                            .highlight()
                            .field("name")
                            .preTags("<em>")
                            .postTags("</em>")
            );
        }
        // 2.1.2.分类过滤
        if(query.getCategory() != null && !query.getCategory().isEmpty()) {
            boolQuery.filter(QueryBuilders.matchQuery("category", query.getCategory()));
        }
        // 2.1.3.品牌过滤
        if(query.getBrand() != null && !query.getBrand().isEmpty()) {
            boolQuery.filter(QueryBuilders.matchQuery("brand", query.getBrand()));
        }
        // 2.1.4.价格过滤
        if(query.getMinPrice() != null || query.getMaxPrice() != null) {
            RangeQueryBuilder rangeQuery = QueryBuilders.rangeQuery("price");
            if (query.getMinPrice() != null) {
                rangeQuery.gte(query.getMinPrice());
            }
            if (query.getMaxPrice() != null) {
                rangeQuery.lte(query.getMaxPrice());
            }
            boolQuery.filter(rangeQuery);
        }

        sourceBuilder.query(boolQuery);

        // 2.2.分页设置
        sourceBuilder.from((query.getPageNo() - 1) * query.getPageSize());

        // 2.3.排序（默认按更新时间降序）
        if(query.getSortBy() != null && !query.getSortBy().isEmpty()) {
            SortOrder order = query.getIsAsc() ? SortOrder.ASC: SortOrder.DESC;
            sourceBuilder.sort(query.getSortBy(), order);
        } else {
            sourceBuilder.sort("updateTime", SortOrder.DESC);
        }

        // 3.发送请求
        SearchResponse response = esClient.search(request, RequestOptions.DEFAULT);

        // 4.返回响应结果
        return parseResponse(response, query.getPageSize());
    }

    /**
     * 解析ES查询响应
     */
    private PageDTO<ItemDTO> parseResponse(SearchResponse response, int pageSize) {
        // 1.命中数据
        SearchHits searchHits = response.getHits();

        // 2. 总条数
        long total = searchHits.getTotalHits().value;

        // 3.获取source
        List<ItemDTO> itemDTOList = new ArrayList<>();
        SearchHit[] hits = searchHits.getHits();
        for (SearchHit hit : hits) {
            // 3.1.获取source结果
            String json = hit.getSourceAsString();
            // 3.2.转为ItemDTO
            ItemDTO itemDTO = JSONUtil.toBean(json, ItemDTO.class);
            // 3.3.处理高亮结果
            Map<String, HighlightField> highlightFields  = hit.getHighlightFields();
            if(highlightFields.containsKey("name")) {
                Text[] fragments = highlightFields.get("name").getFragments();
                if(fragments != null && fragments.length > 0) {
                    // 3.3.1.拼接所有片段为完整的高亮结果
                    StringBuilder hfNameBuilder = new StringBuilder();
                    for(Text fragment : fragments) {
                        hfNameBuilder.append(fragment.toString());
                    }
                    String hfName = hfNameBuilder.toString();
                    // 3.3.2.覆盖非高亮结果
                    itemDTO.setName(hfName);
                }
            }
            itemDTOList.add(itemDTO);
        }

        return new PageDTO<>(total, getPages(total, pageSize), itemDTOList);
    }

    private Long getPages(long total, int pageSize) {
        return (total + pageSize - 1) / pageSize;
    }
}
