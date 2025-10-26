package com.hmall.item.es;

import cn.hutool.core.bean.BeanUtil;
import cn.hutool.json.JSONUtil;
import com.baomidou.mybatisplus.extension.plugins.pagination.Page;
import com.hmall.item.domain.po.Item;
import com.hmall.item.domain.po.ItemDoc;
import com.hmall.item.service.IItemService;
import org.apache.http.HttpHost;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.delete.DeleteRequest;
import org.elasticsearch.action.get.GetRequest;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.text.Text;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.search.aggregations.Aggregation;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.search.aggregations.Aggregations;
import org.elasticsearch.search.aggregations.bucket.terms.Terms;
import org.elasticsearch.search.aggregations.metrics.Stats;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.search.fetch.subphase.highlight.HighlightBuilder;
import org.elasticsearch.search.fetch.subphase.highlight.HighlightField;
import org.elasticsearch.search.sort.SortOrder;
import org.elasticsearch.xcontent.XContentType;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;

import java.io.IOException;
import java.util.List;
import java.util.Map;

//@SpringBootTest
public class ElasticSearchTest {

    private RestHighLevelClient client;

    @Test
    void testMatchAll() throws IOException {
        // 1.准备请求对象
        SearchRequest request = new SearchRequest("items");
        // 2.配置请求参数
        request.source()
                .query(QueryBuilders.matchAllQuery());
        // 3.发送请求
        SearchResponse response = client.search(request, RequestOptions.DEFAULT);
        // 4.解析响应结果
        parseResponseResult(response);
    }

    @Test
    void testSearch() throws IOException {
        // 1.准备请求对象
        SearchRequest request = new SearchRequest("items");
        // 2.组织DSL参数
        request.source()
                .query(QueryBuilders.boolQuery()
                        .must(QueryBuilders.matchQuery("name", "脱脂牛奶"))
                        .filter(QueryBuilders.termQuery("brand", "德亚"))
                        .filter(QueryBuilders.rangeQuery("price").lt(10000))
                );
        // 3.发送请求
        SearchResponse response = client.search(request, RequestOptions.DEFAULT);
        // 4.解析响应结果
        parseResponseResult(response);
    }

    @Test
    void testSortAndPage() throws IOException {
        // 0.模拟前端传递的分页参数
        int PageNo = 1, PageSize = 5;
        // 1.创建请求对象
        SearchRequest request = new SearchRequest("items");

        // 2.组织DSL参数
        // 2.1.query条件
        request.source().query(QueryBuilders.matchAllQuery());
        // 2.2.分页
        request.source().from((PageNo - 1) * PageSize).size(PageSize);
        // 2.3.排序
        request.source()
                .sort("sold", SortOrder.DESC)
                .sort("price", SortOrder.ASC);

        // 3.发送请求
        SearchResponse response = client.search(request, RequestOptions.DEFAULT);
        // 4.解析响应结果
        parseResponseResult(response);
    }

    @Test
    void testHighlight() throws IOException {
        // 1.创建请求对象
        SearchRequest request = new SearchRequest("items");

        // 2.组织DSL参数
        // 2.1.query条件
        request.source().query(QueryBuilders.matchQuery("name", "脱脂牛奶"));
        // 2.2.设置highlight
        request.source().highlighter(SearchSourceBuilder.highlight().field("name"));

        // 3.发送请求
        SearchResponse response = client.search(request, RequestOptions.DEFAULT);
        // 4.解析响应结果
        parseResponseResult(response);
    }

    @Test
    void testAgg() throws IOException {
        // 1.创建请求对象
        SearchRequest request = new SearchRequest("items");

        // 2.组织DSL参数
        // 2.1.分页
        request.source().size(0); // 不返回原始文档，只返回聚合结果
        // 2.2.聚合
        String brandAggName = "brandAgg";
        String priceStatsName = "priceStats";
        request.source().aggregation(
                AggregationBuilders
                        .terms(brandAggName)
                        .field("brand")
                        .size(10)
                        .subAggregation(
                                AggregationBuilders.stats(priceStatsName).field("price")
                        )
        );

        // 3.发送请求
        SearchResponse response = client.search(request, RequestOptions.DEFAULT);

        // 4.解析响应结果
        Aggregations aggregations = response.getAggregations();
        // 4.1.根据聚合名称获取对应的聚合
        Terms brandTerms = aggregations.get(brandAggName);
        // 4.2.获取buckets
        List<? extends Terms.Bucket> buckets = brandTerms.getBuckets();
        // 4.3.遍历获取每一个bucket
        for (Terms.Bucket bucket : buckets) {
            System.out.println("key: " + bucket.getKeyAsString());
            System.out.println("doc_count: " + bucket.getDocCount());
            Stats priceStats = bucket.getAggregations().get(priceStatsName);
            System.out.println("avg: " + priceStats.getAvg());
            System.out.println("---------------------");
        }
    }

    private static void parseResponseResult(SearchResponse response) {
        SearchHits searchHits = response.getHits();
        // 4.1 获得总条数
        long total = searchHits.getTotalHits().value;
        System.out.println("total = " + total);

        // 4.2 命中数据
        SearchHit[] hits = searchHits.getHits();
        for (SearchHit hit : hits) {
            // 4.2.1.获取source结果
            String json = hit.getSourceAsString();
            // 4.2.2.转为ItemDoc
            ItemDoc itemDoc = JSONUtil.toBean(json, ItemDoc.class);
            // 4.3.处理高亮结果
            Map<String, HighlightField> hfs = hit.getHighlightFields();
            if(hfs != null && !hfs.isEmpty()) {
                // 4.3.1.获取高亮结果
                HighlightField hf = hfs.get("name");
                Text[] fragments = hf.getFragments();
                if(fragments != null && fragments.length > 0) {
                    // 4.3.2.拼接所有片段为完整的高亮结果
                    StringBuilder hfNameBuilder = new StringBuilder();
                    for(Text fragment : fragments) {
                        hfNameBuilder.append(fragment.toString());
                    }
                    String hfName = hfNameBuilder.toString();
                    // 4.3.3.覆盖非高亮结果
                    itemDoc.setName(hfName);
                }
            }
            System.out.println("itemDoc = " + itemDoc);
        }
    }

    @BeforeEach
    void setUp() {
        client = new RestHighLevelClient(RestClient.builder(
                HttpHost.create("http://192.168.43.143:9200")
        ));
    }

    @AfterEach
    void tearDown() throws IOException {
        client.close();
    }
}
