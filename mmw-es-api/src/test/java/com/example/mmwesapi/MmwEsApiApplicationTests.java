package com.example.mmwesapi;

import com.alibaba.fastjson.JSON;
import com.example.mmwesapi.Entity.User;
import org.apache.lucene.util.QueryBuilder;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexRequest;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.get.GetRequest;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.client.indices.CreateIndexRequest;
import org.elasticsearch.client.indices.CreateIndexResponse;
import org.elasticsearch.client.indices.GetIndexRequest;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.index.query.TermQueryBuilder;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.search.fetch.subphase.FetchSourceContext;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.boot.test.context.SpringBootTest;

import java.io.IOException;
import java.util.ArrayList;
import java.util.concurrent.TimeUnit;

@SpringBootTest
class MmwEsApiApplicationTests {
    @Autowired
    @Qualifier("restHighLevelClient")
    private RestHighLevelClient client;

    @Test
    void contextLoads() {
    }

    @Test
    void testCreateIndex() throws IOException {
        //执行创建请求
        CreateIndexRequest request = new CreateIndexRequest("mmw_index");
        CreateIndexResponse createIndexResponse = client.indices().create(request, RequestOptions.DEFAULT);
        System.out.println(createIndexResponse);

    }

    @Test
    void exist() throws IOException {
        GetIndexRequest request = new GetIndexRequest("mmw_index");
        System.out.println(client.indices().exists(request, RequestOptions.DEFAULT));
    }

    @Test
    void delte() throws IOException {
        DeleteIndexRequest deleteIndexRequest = new DeleteIndexRequest("mmw_index");
        AcknowledgedResponse delete = client.indices().delete(deleteIndexRequest, RequestOptions.DEFAULT);
        System.out.println(delete.isAcknowledged());

    }

    @Test
    void testAddDoc() throws IOException {
        User user = new User("mmw",26);
        IndexRequest mmw_index = new IndexRequest("mmw_index");
        mmw_index.id("1");
        mmw_index.timeout("1s");
        //放入数据
        mmw_index.source(JSON.toJSONString(user), XContentType.JSON);
        IndexResponse response = client.index(mmw_index, RequestOptions.DEFAULT);
        System.out.println(response);

    }

    @Test
    void testIsExist() throws IOException {
        GetRequest mmw_index = new GetRequest("mmw_index", "1");
        mmw_index.fetchSourceContext(new FetchSourceContext(false));
        mmw_index.storedFields("_none_");
        boolean exists = client.exists(mmw_index, RequestOptions.DEFAULT);
        System.out.println(exists);
    }

    @Test
    void testGetDoc() throws IOException {
        GetRequest mmw_index = new GetRequest("mmw_index", "1");
        GetResponse documentFields = client.get(mmw_index, RequestOptions.DEFAULT);
        System.out.println(documentFields.getSourceAsString());
    }
//批量更新和删除
    @Test
    void testBulkRequest() throws IOException {
        BulkRequest bulkRequest = new BulkRequest();
        bulkRequest.timeout("10s");
        ArrayList<Object> objects = new ArrayList<>();
        objects.add(new User("hello", 20));
        objects.add(new User("hello", 20));
        objects.add(new User("hello", 20));
        objects.add(new User("hello", 20));
        objects.add(new User("world", 20));

        for (int i = 0; i < objects.size(); i++) {
            bulkRequest.add(new IndexRequest("mmw_index")
                    .id("" + (i + 1))
                    .source(JSON.toJSONString(objects.get(i)),XContentType.JSON));
        }
        BulkResponse bulk = client.bulk(bulkRequest, RequestOptions.DEFAULT);
        System.out.println(bulk.hasFailures());
    }

    //查询
    @Test
    void testQuery() throws IOException {
        SearchRequest mmw_index = new SearchRequest("mmw_index");
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
        //匹配所有QueryBuilders.matchAllQuery()
        TermQueryBuilder termQueryBuilder = QueryBuilders.termQuery("name", "hello");
        searchSourceBuilder.query(termQueryBuilder);
        searchSourceBuilder.timeout(new TimeValue(60, TimeUnit.SECONDS));
        mmw_index.source(searchSourceBuilder);
        SearchResponse search = client.search(mmw_index, RequestOptions.DEFAULT);
        System.out.println(JSON.toJSONString(search.getHits()));
        System.out.println("====================================");
        for (SearchHit searchHit : search.getHits().getHits()) {
            System.out.println(searchHit.getSourceAsMap());
        }
    }
}
