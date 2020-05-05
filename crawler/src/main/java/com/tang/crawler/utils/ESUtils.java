package com.tang.crawler.utils;

import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import org.elasticsearch.action.admin.indices.create.CreateIndexRequestBuilder;
import org.elasticsearch.action.delete.DeleteResponse;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.get.MultiGetItemResponse;
import org.elasticsearch.action.get.MultiGetResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexRequestBuilder;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.search.SearchRequestBuilder;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.action.update.UpdateResponse;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.text.Text;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.search.fetch.subphase.highlight.HighlightBuilder;
import org.elasticsearch.search.fetch.subphase.highlight.HighlightField;
import org.elasticsearch.transport.client.PreBuiltTransportClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

public class ESUtils {

    Logger logger = LoggerFactory.getLogger(ESUtils.class);
    /* @Autowired
    private static TransportClient client;*/

    private static TransportClient client;

    static {
        try{
            // 1 设置连接的集群名称
            Settings settings = Settings.builder().put("cluster.name", "MYELK").build();
            // 2 连接集群
            client = new PreBuiltTransportClient(settings);
            client.addTransportAddress(new InetSocketTransportAddress(InetAddress.getByName("ELK05"), 9300));
        }catch (Exception e){
        }
    }

    /**
        * @Description: 创建索引
    　　* @author tang
    　　* @date 2019/2/6 14:53
    　　*/
    public static void createIndex(String indexNames,String type) throws Exception{

        XContentBuilder mapping = createMapping(indexNames, type);
        // 1 创建索引.
        CreateIndexRequestBuilder indexRequestBuilder = client.admin().indices().prepareCreate(indexNames).addMapping(type,mapping);
        indexRequestBuilder.get();

    }
    /**
        * @Description: 创建mapping 并配置分词器
    　　* @author tang
    　　* @date 2019/3/9 19:39
    　　*/
    public static XContentBuilder createMapping(String index,String type) throws Exception {

        // 1设置mapping
        XContentBuilder builder = XContentFactory.jsonBuilder()
                .startObject()
                .startObject(type)
                .startObject("properties")
                .startObject("spu_name")
                .field("type", "text")
                .field("analyzer", "ik_max_word")
                .field("search_analyzer","ik_smart")
                .endObject()
                .startObject("shop_name")
                .field("type", "text")
                .field("analyzer", "ik_max_word")
                .field("search_analyzer","ik_smart")
                .endObject()
                .endObject()
                .endObject()
                .endObject();

        return builder;
      /*  // 2 添加mapping
        PutMappingRequest mapping = Requests.putMappingRequest(index).type(type).source(builder);

        client.admin().indices().putMapping(mapping).get();

        // 3 关闭资源
        client.close();*/
    }

    /**
        * @Description: 删除索引
    　　* @author tang
    　　* @date 2019/2/6 14:55
    　　*/
    public  void deleteIndex(String... indexNames){
        logger.info("===deleteIndex===inde");
        // 1 删除索引
        client.admin().indices().prepareDelete(indexNames).get();

    }

    /**
        * @Description: 创建索引json格式数据
    　　* @author tang
    　　* @date 2019/2/6 15:04
    　　*/
    public static void createIndexByJson(String index,String type,String id,String json) throws UnknownHostException {
        // 1 文档数据准备
        // 2 创建文档
        IndexRequestBuilder indexRequestBuilder = client.prepareIndex(index, type, id);
        IndexResponse indexResponse =indexRequestBuilder.setSource(json).execute().actionGet();
        String result = indexResponse.toString();
        System.out.println(result);
    }

    /**
        * @Description: 创建索引map格式数据
    　　* @author tang
    　　* @date 2019/2/6 15:07
    　　*/
    public  void createIndexByMap(String index,String type,String id,Map map) {
        // 1 文档数据准备
        // 2 创建文档
        IndexResponse indexResponse = client.prepareIndex(index,type,id).setSource(map).execute().actionGet();

    }

    /**
        * @Description: 简单查询查询
    　　* @author tang
    　　* @date 2019/2/6 15:10
    　　*/
    public String getData(String index,String type,String id) throws Exception {
        String sourceAsString=null;
        // 1 查询文档
        GetResponse response = client.prepareGet(index,type,id).get();
        // 2 打印搜索的结果
        sourceAsString = response.getSourceAsString();
        System.out.println(sourceAsString);

        return sourceAsString;
    }

    public  String getMultiData(String index,String type,String... ids) {
        JSONArray jsonArray = new JSONArray();
        MultiGetResponse responses = client.prepareMultiGet().add(index, type, ids).get();
        for(MultiGetItemResponse itemResponse:responses){
            GetResponse getResponse = itemResponse.getResponse();
            // 如果获取到查询结果
            if (getResponse.isExists()) {
                String sourceAsString = getResponse.getSourceAsString();
                jsonArray.add(sourceAsString);
            }
        }
        return jsonArray.toString();
    }

    /**
        * @Description: 更新数据
    　　* @author tang
    　　* @date 2019/2/6 15:25
    　　*/
    public  void updateData(String index,String type,String id) throws Throwable {
        // 1 创建更新数据的请求对象
        UpdateRequest updateRequest = new UpdateRequest();
        updateRequest.index(index);
        updateRequest.type(type);
        updateRequest.id(id);
        updateRequest.doc(XContentFactory.jsonBuilder().startObject()
                // 对没有的字段添加, 对已有的字段替换
                .field("title", "基于Lucene的搜索服务器")
                .field("content",
                        "它提供了一个分布式多用户能力的全文搜索引擎，基于RESTful web接口。大数据前景无限")
                .endObject());
        // 2 获取更新后的值
        UpdateResponse indexResponse = client.update(updateRequest).get();
        // 3 打印返回的结果
        System.out.println("index:" + indexResponse.getIndex());
        System.out.println("type:" + indexResponse.getType());
        System.out.println("id:" + indexResponse.getId());
        System.out.println("version:" + indexResponse.getVersion());

    }

    public  void testUpsert() throws Exception {

        // 设置查询条件, 查找不到则添加
        IndexRequest indexRequest = new IndexRequest("blog", "article", "5")
                .source(XContentFactory.jsonBuilder().startObject().field("title", "搜索服务器").field("content","它提供了一个分布式多用户能力的全文搜索引擎，基于RESTful web接口。Elasticsearch是用Java开发的，并作为Apache许可条款下的开放源码发布，是当前流行的企业级搜索引擎。设计用于云计算中，能够达到实时搜索，稳定，可靠，快速，安装使用方便。").endObject());

        // 设置更新, 查找到更新下面的设置
        UpdateRequest upsert = new UpdateRequest("blog", "article", "5")
                .doc(XContentFactory.jsonBuilder().startObject().field("user", "李四").endObject()).upsert(indexRequest);
        client.update(upsert).get();
        client.close();
    }

    /**
        * @Description: 删除索引
    　　* @author tang
    　　* @date 2019/2/6 15:31
    　　*/
    public  void deleteData(String index,String type,String id) {
        //打印日志
        logger.info("===deleteData===index="+index+"===type="+type+"===id="+id);
        // 1 删除文档数据
        DeleteResponse indexResponse = client.prepareDelete(index,type,id).get();
    }

    /**
        * @Description: 查询指定index和type中的所用记录，相当于sql：select * from sales
    　　* @author tang
    　　* @date 2019/2/6 15:33
    　　*/
    public  String matchAllQuery(String type,String ...index) {
        // 1 执行查询
        SearchResponse searchResponse = client.prepareSearch(index).setTypes(type)
                .setQuery(QueryBuilders.matchAllQuery()).get();
        // 2 打印查询结果
        SearchHits hits = searchResponse.getHits(); // 获取命中次数，查询结果有多少对象
        System.out.println("查询结果有：" + hits.getTotalHits() + "条");
        Iterator<SearchHit> iterator = hits.iterator();
        while (iterator.hasNext()) {
            SearchHit searchHit = iterator.next(); // 每个查询对象
            System.out.println(searchHit.getSourceAsString()); // 获取字符串格式打印
        }
        return null;
    }
    /**
        * @Description: 匹配查询
        * matchQuery可以简单理解为mysql中的like，
        * 但是我不知道我这么理解对不对，因为在elasticsearch中使用matchQuery查询时，他会对查询的field进行分词，
        * 打个比方，我们搜索"联想笔记本电脑"，他可能会将他拆分为：“联想”，“电脑”，“联想电脑”，
        * 那么如果一个filed中包括 联想 两个字就可以被搜出来。
        * 当然我们进行查询的这个field的mapping必须是text类型。（如果是中文分词的话，还需要配置中文分词器），他的查询语句和上边基本相似
    　　* @author tang
    　　* @date 2019/2/6 15:33
    　　*/
    public String matchQuery(String type,String searchType,String searchContent,String ...index) {
        // 1 执行查询
        SearchRequestBuilder searchRequestBuilder = client.prepareSearch(index).setTypes(type)
                .setQuery(QueryBuilders.matchQuery(searchType,searchContent)).setFrom(0).setSize(10);

        HighlightBuilder highlightBuilder = new HighlightBuilder().field("*").requireFieldMatch(false);
        highlightBuilder.preTags("<span style=\"color:red\">");
        highlightBuilder.postTags("</span>");
        searchRequestBuilder.highlighter(highlightBuilder);
        SearchResponse searchResponse =  searchRequestBuilder.get();
        // 2 打印查询结果
        SearchHits hits = searchResponse.getHits(); // 获取命中次数，查询结果有多少对象
        System.out.println("查询结果有：" + hits.getTotalHits() + "条");
        Iterator<SearchHit> iterator = hits.iterator();
        while (iterator.hasNext()) {
            SearchHit searchHit = iterator.next(); // 每个查询对象
            Map<String, HighlightField> highlightFields = searchHit.getHighlightFields();
            HighlightField highlightField = highlightFields.get(searchType);
            Text[] texts = highlightField.getFragments();
            String title = "";
            for (Text text : texts) {
                title += text;
            }
            String sourceAsString = searchHit.getSourceAsString();
            JSONObject jsonObject = JSONObject.parseObject(sourceAsString);
            jsonObject.put(searchType,title);
            sourceAsString=jsonObject.toString();
            System.out.println(sourceAsString);
        }
        return null;
    }
   /**
       * @Description: 多字段搜索
   　　* @author tang
   　　* @date 2019/3/2 17:19
   　　*/
    public static List<String> multiMatchQuery(String index,String type,String textContent,Integer start,Integer size,String...fieldNames) {

        List<String> resultList = new ArrayList<String>();
        // 1 执行查询
        SearchRequestBuilder searchRequestBuilder = client.prepareSearch(index).setTypes(type)
                .setQuery(QueryBuilders.multiMatchQuery(textContent,fieldNames)).setFrom(start).setSize(size);

        HighlightBuilder highlightBuilder = new HighlightBuilder().field("*").requireFieldMatch(false);
        highlightBuilder.preTags("<span style=\"color:red\">");
        highlightBuilder.postTags("</span>");
        searchRequestBuilder.highlighter(highlightBuilder);
        SearchResponse searchResponse =  searchRequestBuilder.get();
        // 2 打印查询结果
        SearchHits hits = searchResponse.getHits(); // 获取命中次数，查询结果有多少对象
        System.out.println("查询结果有：" + hits.getTotalHits() + "条");
        Iterator<SearchHit> iterator = hits.iterator();
        while (iterator.hasNext()) {
            SearchHit searchHit = iterator.next(); // 每个查询对象
            String sourceAsString = searchHit.getSourceAsString();
            Map<String, HighlightField> highlightFields = searchHit.getHighlightFields();
            if(highlightFields.containsKey(fieldNames[0])){
                HighlightField highlightField = highlightFields.get(fieldNames[0]);
                Text[] texts = highlightField.getFragments();
                String title = "";
                for (Text text : texts) {
                    title += text;
                }
                JSONObject jsonObject = JSONObject.parseObject(sourceAsString);
                jsonObject.put(fieldNames[0],title);
                sourceAsString=jsonObject.toString();
            }

            if(highlightFields.containsKey(fieldNames[1])){
                HighlightField highlightField = highlightFields.get(fieldNames[1]);
                Text[] texts = highlightField.getFragments();
                String title = "";
                for (Text text : texts) {
                    title += text;
                }
                JSONObject jsonObject = JSONObject.parseObject(sourceAsString);
                jsonObject.put(fieldNames[1],title);
                sourceAsString=jsonObject.toString();

            }
            System.out.println(sourceAsString);
            resultList.add(sourceAsString);
        }
        // 3 关闭连接
        return resultList;
    }
    /**
        * @Description: 对所有字段分词查询
    　　* @author tang
    　　* @date 2019/2/6 15:36
    　　*/
    public  String query(String type,String content,String...indexs) {
        // 1 条件查询
        SearchRequestBuilder searchRequestBuilder = client.prepareSearch(indexs).setTypes(type)
                .setQuery(QueryBuilders.queryStringQuery(content));
        //设置高亮显示
        HighlightBuilder highlightBuilder = new HighlightBuilder().field("*").requireFieldMatch(false);
        highlightBuilder.preTags("<span style=\"color:red\">");
        highlightBuilder.postTags("</span>");
        searchRequestBuilder.highlighter(highlightBuilder);
        SearchResponse searchResponse =  searchRequestBuilder.get();
        // 2 打印查询结果
        SearchHits hits = searchResponse.getHits(); // 获取命中次数，查询结果有多少对象
        System.out.println("查询结果有：" + hits.getTotalHits() + "条");
        Iterator<SearchHit> iterator = hits.iterator();
        while (iterator.hasNext()) {
            SearchHit searchHit = iterator.next(); // 每个查询对象
            Map<String, HighlightField> highFields = searchHit.highlightFields();
            System.out.println(highFields);
            System.out.println(searchHit.getSourceAsString()); // 获取字符串格式打印
        }
        // 3 关闭连接
        client.close();
        return null;
    }
    /**
        * @Description: 通配符查询
    　　* @author tang
    　　* @date 2019/2/6 15:39
    　　*/
    public static String wildcardQuery(String type,String content,String contentType,String... indexs) {
        // 1 通配符查询
        SearchResponse searchResponse = client.prepareSearch(indexs).setTypes(type)
                .setQuery(QueryBuilders.wildcardQuery(contentType, "*"+content+"*")).get();
        // 2 打印查询结果
        SearchHits hits = searchResponse.getHits(); // 获取命中次数，查询结果有多少对象
        System.out.println("查询结果有：" + hits.getTotalHits() + "条");
        Iterator<SearchHit> iterator = hits.iterator();
        while (iterator.hasNext()) {
            SearchHit searchHit = iterator.next(); // 每个查询对象
            System.out.println(searchHit.getSourceAsString()); // 获取字符串格式打印
        }
        // 3 关闭连接
        client.close();
        return null;
    }
    /**
        * @Description: 词条查询 相当于sql中的"="号
    　　* @author tang
    　　* @date 2019/2/6 15:49
    　　*/
    public static String termQuery(String searchType,String searchContent,String type,String...indexs) {
        // 1 第一field查询
        SearchRequestBuilder searchRequestBuilder = client.prepareSearch(indexs).setTypes(type)
                .setQuery(QueryBuilders.termQuery(searchType, searchContent)).setFrom(0).setSize(20);
        //设置高亮显示
        HighlightBuilder highlightBuilder = new HighlightBuilder().field("*").requireFieldMatch(false);
        highlightBuilder.preTags("<span style=\"color:red\">");
        highlightBuilder.postTags("</span>");
        searchRequestBuilder.highlighter(highlightBuilder);
        SearchResponse searchResponse =  searchRequestBuilder.get();
        // 2 打印查询结果
        SearchHits hits = searchResponse.getHits(); // 获取命中次数，查询结果有多少对象
        System.out.println("查询结果有：" + hits.getTotalHits() + "条");
        Iterator<SearchHit> iterator = hits.iterator();
        while (iterator.hasNext()) {
            SearchHit searchHit = iterator.next(); // 每个查询对象
            System.out.println(searchHit.getSourceAsString()); // 获取字符串格式打印
        }
        // 3 关闭连接
        client.close();
        return null;
    }

    /**
        * @Description: 模糊查询
    　　* @author tang
    　　* @date 2019/2/6 15:50
    　　*/
    public static String fuzzy() {
        // 1 模糊查询
        SearchResponse searchResponse = client.prepareSearch("blog").setTypes("article")
                .setQuery(QueryBuilders.fuzzyQuery("title", "lucene")).get();
        // 2 打印查询结果
        SearchHits hits = searchResponse.getHits(); // 获取命中次数，查询结果有多少对象
        System.out.println("查询结果有：" + hits.getTotalHits() + "条");
        Iterator<SearchHit> iterator = hits.iterator();
        while (iterator.hasNext()) {
            SearchHit searchHit = iterator.next(); // 每个查询对象
            System.out.println(searchHit.getSourceAsString()); // 获取字符串格式打印
        }
        // 3 关闭连接
        client.close();
        return null;
    }


    public static void main(String[] args) throws Exception{

       /* String result = matchQuery("item", "title", "联想", "e3mall");
        System.out.println(result);*/
     /*  multiMatchQuery("e3mall-1","item","芦柑辣椒",0,20,"title","sell_point");*/

       createIndex("item","2020-03-25");

    }
}