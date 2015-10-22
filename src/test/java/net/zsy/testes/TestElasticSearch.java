package net.zsy.testes;

import java.io.IOException;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.text.DateFormat;
import java.util.Date;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

import org.elasticsearch.action.bulk.BulkItemResponse;
import org.elasticsearch.action.bulk.BulkProcessor;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.count.CountRequestBuilder;
import org.elasticsearch.action.count.CountResponse;
import org.elasticsearch.action.delete.DeleteResponse;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.get.MultiGetItemResponse;
import org.elasticsearch.action.get.MultiGetResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexRequestBuilder;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.search.MultiSearchRequestBuilder;
import org.elasticsearch.action.search.MultiSearchResponse;
import org.elasticsearch.action.search.SearchRequestBuilder;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.SearchType;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.action.update.UpdateResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.common.unit.ByteSizeUnit;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.search.aggregations.bucket.histogram.DateHistogramInterval;
import org.elasticsearch.search.aggregations.bucket.terms.Terms;
import org.elasticsearch.search.aggregations.bucket.terms.TermsBuilder;
import org.elasticsearch.search.aggregations.metrics.avg.Avg;
import org.elasticsearch.search.aggregations.metrics.avg.AvgBuilder;
import org.elasticsearch.search.aggregations.metrics.cardinality.Cardinality;
import org.elasticsearch.search.aggregations.metrics.cardinality.CardinalityBuilder;
import org.elasticsearch.search.aggregations.metrics.max.Max;
import org.elasticsearch.search.aggregations.metrics.max.MaxBuilder;
import org.elasticsearch.search.aggregations.metrics.min.Min;
import org.elasticsearch.search.aggregations.metrics.min.MinBuilder;
import org.elasticsearch.search.aggregations.metrics.percentiles.Percentile;
import org.elasticsearch.search.aggregations.metrics.percentiles.PercentileRanks;
import org.elasticsearch.search.aggregations.metrics.percentiles.PercentileRanksBuilder;
import org.elasticsearch.search.aggregations.metrics.percentiles.Percentiles;
import org.elasticsearch.search.aggregations.metrics.percentiles.PercentilesBuilder;
import org.elasticsearch.search.aggregations.metrics.stats.Stats;
import org.elasticsearch.search.aggregations.metrics.stats.StatsBuilder;
import org.elasticsearch.search.aggregations.metrics.stats.extended.ExtendedStats;
import org.elasticsearch.search.aggregations.metrics.stats.extended.ExtendedStatsBuilder;
import org.elasticsearch.search.aggregations.metrics.sum.Sum;
import org.elasticsearch.search.aggregations.metrics.sum.SumBuilder;
import org.elasticsearch.search.aggregations.metrics.tophits.TopHits;
import org.elasticsearch.search.aggregations.metrics.valuecount.ValueCount;
import org.elasticsearch.search.aggregations.metrics.valuecount.ValueCountBuilder;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;

public class TestElasticSearch {

	private static final String addr = "192.168.100.101";

	Client client;

	@Before
	public void initClient() {
		Settings settings = Settings.settingsBuilder().put("cluster.name", "escluster").build();
		TransportClient transportClient = TransportClient.builder().settings(settings).build();
		try {
			transportClient.addTransportAddress(new InetSocketTransportAddress(InetAddress.getByName(addr), 9300));
			client = transportClient;
		} catch (UnknownHostException e) {
			e.printStackTrace();
		}
	}

	/**
	 * 使用JAVABEAN创建索引
	 */
	@Test
	public void testCreateIndexUseBean() {
		List<User> users = User.getUserBuilder().genUsers(10000);
		ObjectMapper mapper = new ObjectMapper();
		mapper.setDateFormat(DateFormat.getDateTimeInstance());

		IndexRequestBuilder builder = client.prepareIndex("indices", User.TYPE);
		for (User user : users) {
			try {
				builder.setId(String.valueOf(user.getId())).setSource(mapper.writeValueAsBytes(user));
			} catch (JsonProcessingException e) {
				e.printStackTrace();
			}
			IndexResponse response = builder.get(TimeValue.timeValueSeconds(3));
			System.out.println("create:" + response.getIndex() + "/" + response.getType() + "/" + response.getId()
					+ " version:" + response.getVersion());
		}
	}

	@Test
	public void createIndexUseMap() {
	}

	/**
	 * 使用XContentBuilder创建索引(推荐)
	 */
	@Test
	public void testCreateIndexUseHelper() {
		List<User> users = User.getUserBuilder().genUsers(1);
		try {
			for (User user : users) {
				XContentBuilder contentbuilder = XContentFactory.jsonBuilder();
				IndexRequestBuilder builder = client.prepareIndex("indices", User.TYPE)
						.setId(String.valueOf(user.getId()))
						.setSource(contentbuilder.startObject().field("id", user.getId()).field("name", user.getName())
								.field("age", user.getAge()).field("gender", user.getGender())
								.field("birthday", user.getBirthday()).field("mobile", user.getMobile())
								.field("hobbies", user.getHobbies()).field("tag_createdatetime", new Date())
								.endObject());

				IndexResponse response = builder.get();
				System.out.println("create:" + response.getIndex() + "/" + response.getType() + "/" + response.getId()
						+ " version:" + response.getVersion());
			}
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	/**
	 * 根据ID获取一个索引文档
	 */
	@Test
	public void testGetIndex() {
		GetResponse response = client.prepareGet("indices", User.TYPE, "597421").setOperationThreaded(false).get();
		System.out.println(response.getSourceAsMap());
	}

	/**
	 * 根据ID删除一个索引文档
	 */
	@Test
	public void testDeleteIndex() {
		DeleteResponse response = client.prepareDelete("indices", User.TYPE, "597421").get();
		System.out.println(response.getContext());
	}

	/**
	 * 更新索引（不存在即创建的写法）
	 */
	@Test
	public void testUpdateIndex() {

		User u = User.getUserBuilder().genUsers(1).get(0);
		ObjectMapper mapper = new ObjectMapper();
		mapper.setDateFormat(DateFormat.getDateTimeInstance());
		try {
			// 先构造一个index请求
			IndexRequest indexRequest = new IndexRequest();
			indexRequest.index("indices");
			indexRequest.type(User.TYPE);
			indexRequest.id("597421");

			// 设置source
			indexRequest.source(mapper.writeValueAsBytes(u));

			// 再构造一个update请求
			UpdateRequest updateRequest = new UpdateRequest();
			updateRequest.index("indices");
			updateRequest.type(User.TYPE);
			updateRequest.id("597421");

			// 还必须得设置文档- -
			updateRequest.doc("id", "597421");

			// 设置upsert为index请求
			updateRequest.upsert(indexRequest);

			UpdateResponse response = client.update(updateRequest).get();
			System.out.println(response.isCreated());// 是否执行的创建操作
			System.out.println("create:" + response.getIndex() + "/" + response.getType() + "/" + response.getId()
					+ " version:" + response.getVersion());

			// prepare方式
			client.prepareUpdate("indices", User.TYPE, "597421").setDoc(mapper.writeValueAsBytes(u))
					.setUpsert(mapper.writeValueAsBytes(u)).get();
		} catch (JsonProcessingException e) {
			e.printStackTrace();
		} catch (InterruptedException e) {
			e.printStackTrace();
		} catch (ExecutionException e) {
			e.printStackTrace();
		}
	}

	/**
	 * 索引多个文档
	 */
	@Test
	public void testMultiGetIndex() {
		MultiGetResponse response = client.prepareMultiGet().add("indices", User.TYPE, "597421", "837407").get();
		for (Iterator<MultiGetItemResponse> it = response.iterator(); it.hasNext();) {
			GetResponse gr = it.next().getResponse();
			if (gr.isExists()) {// 文档是否存在
				System.out.println(gr.getSourceAsString());
			}
		}
	}

	/**
	 * 使用Bulk API一次发送多个类型的请求(DELETE/UPDATE/INDEX)
	 */
	@Test
	public void testBulkRequest() {
		User u = User.getUserBuilder().genUsers(1).get(0);
		ObjectMapper mapper = new ObjectMapper();
		mapper.setDateFormat(DateFormat.getDateTimeInstance());
		try {
			BulkResponse response = client.prepareBulk()
					.add(client.prepareIndex("indices", User.TYPE, String.valueOf(u.getId()))
							.setSource(mapper.writeValueAsBytes(u)))
					.add(client.prepareDelete("indices", User.TYPE, "837407")).get();
			System.out.println("Has filure:" + response.hasFailures());
			for (Iterator<BulkItemResponse> it = response.iterator(); it.hasNext();) {
				BulkItemResponse b = it.next();
				System.out.println(b.isFailed());
				System.out.println(b.getOpType() + ":" + b.getIndex() + "/" + b.getType() + "/" + b.getId()
						+ " version:" + b.getVersion());
			}

		} catch (JsonProcessingException e) {
			e.printStackTrace();
		}
	}

	@Test
	public void testBulkProcessor() {
		BulkProcessor bulkProcessor = BulkProcessor.builder(client, new BulkProcessor.Listener() {

			public void beforeBulk(long executionId, BulkRequest request) {
				System.out.println("Before bulk:" + executionId + request.numberOfActions());
			}

			public void afterBulk(long executionId, BulkRequest request, Throwable failure) {
				System.out.println("After bulk with failure:" + executionId + request.numberOfActions());
				failure.printStackTrace();
			}

			public void afterBulk(long executionId, BulkRequest request, BulkResponse response) {
				System.out.println("After bulk:" + executionId + request.numberOfActions());
				for (Iterator<BulkItemResponse> it = response.iterator(); it.hasNext();) {
					BulkItemResponse b = it.next();
					System.out.println(b.isFailed());
					System.out.println(b.getOpType() + ":" + b.getIndex() + "/" + b.getType() + "/" + b.getId()
							+ " version:" + b.getVersion());
				}
			}
		}).setBulkActions(1000).setBulkSize(new ByteSizeValue(5, ByteSizeUnit.MB))
				.setFlushInterval(TimeValue.timeValueSeconds(2)).setConcurrentRequests(1).build();

		User u = User.getUserBuilder().genUsers(1).get(0);
		ObjectMapper mapper = new ObjectMapper();
		mapper.setDateFormat(DateFormat.getDateTimeInstance());
		try {
			bulkProcessor.add(new IndexRequest("indices", User.TYPE, String.valueOf(u.getId()))
					.source(mapper.writeValueAsBytes(u)));
		} catch (JsonProcessingException e) {
			e.printStackTrace();
		}
		try {
			bulkProcessor.awaitClose(1, TimeUnit.MINUTES);
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
	}

	@Test
	public void testSearchIndex() {
		// 查询全部
		// client.prepareSearch().get();
		SearchRequestBuilder searchRequestBuilder = client.prepareSearch("indices").setTypes(User.TYPE)
				.setSearchType(SearchType.QUERY_THEN_FETCH)
				.setQuery(QueryBuilders.boolQuery().must(QueryBuilders.matchPhraseQuery("name", "Monkey D Luffy"))
						.must(QueryBuilders.matchPhraseQuery("gender", "male")))
				.setPostFilter(QueryBuilders.rangeQuery("age").from(0).to(20)).setExplain(true).setFrom(0).setSize(3);

		System.out.println(searchRequestBuilder.toString());
		SearchResponse response = searchRequestBuilder.get();
		SearchHits searchHits = response.getHits();
		for (Iterator<SearchHit> it = searchHits.iterator(); it.hasNext();) {
			SearchHit sh = it.next();
			System.out.println(sh.getSourceAsString());
		}
	}

	@Test
	public void testScroll() {
		SearchResponse response = client.prepareSearch("indices").setSearchType(SearchType.SCAN)
				.setScroll(new TimeValue(60000))
				.setQuery(QueryBuilders.boolQuery().must(QueryBuilders.matchPhraseQuery("name", "Monkey D Luffy")))
				.setSize(3).get();
		while (true) {
			System.out.println(response.getScrollId());
			for (SearchHit hit : response.getHits()) {
				System.out.println(hit.getSourceAsString());
			}
			response = client.prepareSearchScroll(response.getScrollId()).setScroll(new TimeValue(60000)).get();
			if (response.getHits().getHits().length == 0)
				break;
		}
	}

	@Test
	public void testMultiSearch() {
		SearchRequestBuilder builder1 = client.prepareSearch().setQuery(QueryBuilders.queryStringQuery("female"))
				.setSize(1);
		SearchRequestBuilder builder2 = client.prepareSearch()
				.setQuery(QueryBuilders.matchQuery("name", "Monkey D Luffy")).setSize(1);

		MultiSearchRequestBuilder requestBuilder = client.prepareMultiSearch().add(builder1).add(builder2);
		MultiSearchResponse response = requestBuilder.get();

		for (MultiSearchResponse.Item item : response.getResponses()) {
			SearchResponse r = item.getResponse();
			for (SearchHit hit : r.getHits()) {
				System.out.println(hit.getSourceAsString());
			}
		}
	}

	@Test
	public void testAggregations() {
		SearchResponse response = client.prepareSearch().setQuery(QueryBuilders.matchAllQuery())
				.addAggregation(AggregationBuilders.terms("agg1").field("age"))
				.addAggregation(AggregationBuilders.dateHistogram("birthday").interval(DateHistogramInterval.YEAR))
				.get();

		Terms agg1 = response.getAggregations().get("agg1");
		for (Iterator<Terms.Bucket> it = agg1.getBuckets().iterator(); it.hasNext();) {
			Terms.Bucket bucket = it.next();
			System.out.println(bucket.getKeyAsString());
			System.out.println(bucket.getDocCount());
		}
	}

	@Test
	public void testTerminateAfter() {
		SearchResponse response = client.prepareSearch("indices").setTerminateAfter(1000)// 1000条数据后终止
				.get();

		System.out.println(response.isTerminatedEarly());
		System.out.println(response.getHits().getTotalHits());
	}

	@Test
	public void testCount() {
		CountRequestBuilder builder = client.prepareCount("indices")
				.setQuery(QueryBuilders.matchPhraseQuery("name", "Monkey D Luffy"));
		System.out.println(builder.toString());
		CountResponse response = builder.get();
		System.out.println(response.getCount());
	}

	@Test
	public void testStructAggregations() {
		SearchRequestBuilder requestBuilder = client.prepareSearch("indices")
				.addAggregation(AggregationBuilders.terms("by_name").field("name")
						.subAggregation(AggregationBuilders.dateHistogram("by_year").field("birthday")
								.format("yyyy-MM-dd HH:mm:ss").interval(DateHistogramInterval.YEAR)));
		System.out.println(requestBuilder.toString());
		SearchResponse response = requestBuilder.get();
		System.out.println(response.getAggregations().getAsMap());
	}

	@Test
	public void testMetricsAggregations() {

		SearchRequestBuilder requestBuilder = null;
		SearchResponse response = null;

		// Min Aggregation
		MinBuilder minBuilder = AggregationBuilders.min("agg").field("age");
		requestBuilder = client.prepareSearch("indices").addAggregation(minBuilder);
		System.out.println(requestBuilder.toString());
		response = requestBuilder.get();
		Min minAgg = response.getAggregations().get("agg");
		System.out.println("min:" + minAgg.getValue());

		// Max Aggregation
		MaxBuilder maxBuilder = AggregationBuilders.max("agg").field("age");
		requestBuilder = client.prepareSearch("indices").addAggregation(maxBuilder);
		System.out.println(requestBuilder.toString());
		response = requestBuilder.get();
		Max maxAgg = response.getAggregations().get("agg");
		System.out.println("max:" + maxAgg.getValue());

		// Sum Aggregation
		SumBuilder sumBuilder = AggregationBuilders.sum("agg").field("age");
		requestBuilder = client.prepareSearch("indices").addAggregation(sumBuilder);
		System.out.println(requestBuilder.toString());
		response = requestBuilder.get();
		Sum sumAgg = response.getAggregations().get("agg");
		System.out.println("sum:" + sumAgg.getValue());

		// Avg Aggregation
		AvgBuilder avgBuilder = AggregationBuilders.avg("agg").field("age");
		requestBuilder = client.prepareSearch("indices").addAggregation(avgBuilder);
		System.out.println(requestBuilder.toString());
		response = requestBuilder.get();
		Avg avgAgg = response.getAggregations().get("agg");
		System.out.println("avg:" + avgAgg.getValue());

		// Stats Aggregation
		StatsBuilder statsBuilder = AggregationBuilders.stats("agg").field("age");
		requestBuilder = client.prepareSearch("indices").addAggregation(statsBuilder);
		System.out.println(requestBuilder.toString());
		response = requestBuilder.get();
		Stats statsAgg = response.getAggregations().get("agg");
		System.out.println("stats min:" + statsAgg.getMin() + " max:" + statsAgg.getMax() + " sum:" + statsAgg.getSum()
				+ " avg:" + statsAgg.getAvg() + " count:" + statsAgg.getCount());

		// Extended Stats Aggregation
		ExtendedStatsBuilder extendedStatsBuilder = AggregationBuilders.extendedStats("agg").field("age");
		requestBuilder = client.prepareSearch("indices").addAggregation(extendedStatsBuilder);
		System.out.println(requestBuilder.toString());
		response = requestBuilder.get();
		ExtendedStats extendedStats = response.getAggregations().get("agg");
		System.out.println("extended stats min:" + extendedStats.getMin() + " max:" + extendedStats.getMax() + " sum:"
				+ extendedStats.getSum() + " avg:" + extendedStats.getAvg() + " count:" + extendedStats.getCount());
		System.out.println("standard deviation:" + extendedStats.getStdDeviation() + " sum of squares:"
				+ extendedStats.getSumOfSquares() + " variance:" + extendedStats.getVariance());

		// Value Count Aggregation
		ValueCountBuilder valueCountBuilder = AggregationBuilders.count("agg").field("age");
		requestBuilder = client.prepareSearch("indices").addAggregation(valueCountBuilder);
		System.out.println(requestBuilder.toString());
		response = requestBuilder.get();
		ValueCount valueCount = response.getAggregations().get("agg");
		System.out.println("value count:" + valueCount.getValue());

		// Percentile Aggregation
		PercentilesBuilder percentilesBuilder = AggregationBuilders.percentiles("agg").field("age");
		requestBuilder = client.prepareSearch("indices").addAggregation(percentilesBuilder);
		System.out.println(requestBuilder.toString());
		response = requestBuilder.get();
		Percentiles percentiles = response.getAggregations().get("agg");

		for (Iterator<Percentile> it = percentiles.iterator(); it.hasNext();) {
			Percentile p = it.next();
			System.out.println("percent:" + p.getPercent() + " value:" + p.getValue());
		}
		// Percentile Ranks Aggregation
		PercentileRanksBuilder percentileRanksBuilder = AggregationBuilders.percentileRanks("agg").field("age")
				.percentiles(20, 25, 50);
		requestBuilder = client.prepareSearch("indices").addAggregation(percentileRanksBuilder);
		System.out.println(requestBuilder.toString());
		response = requestBuilder.get();

		PercentileRanks percentileRanks = response.getAggregations().get("agg");
		for (Percentile p : percentileRanks) {
			System.out.println("percent:" + p.getPercent() + " value:" + p.getValue());
		}

		// Cardinality Aggregation 基数
		CardinalityBuilder cardinalityBuilder = AggregationBuilders.cardinality("agg").field("name");
		requestBuilder = client.prepareSearch("indices").addAggregation(cardinalityBuilder);
		System.out.println(requestBuilder.toString());
		response = requestBuilder.get();

		Cardinality cardinality = response.getAggregations().get("agg");
		System.out.println(cardinality.getValue());

		// Ceo Bounds Aggregation 地理边界

		// Top Hits
		TermsBuilder termsBuilder = AggregationBuilders.terms("agg").field("gender")
				.subAggregation(AggregationBuilders.topHits("top"));
		requestBuilder = client.prepareSearch("indices").addAggregation(termsBuilder);
		System.out.println(requestBuilder.toString());
		response = requestBuilder.get();

		Terms terms = response.getAggregations().get("agg");
		for (Terms.Bucket entry : terms.getBuckets()) {
			System.out.println("key:" + entry.getKey() + " doc_cout:" + entry.getDocCount());

			TopHits topHits = entry.getAggregations().get("top");
			for (SearchHit hit : topHits.getHits().getHits()) {
				System.out.println(" ->id " + hit.getId() + ",_source:" + hit.getSourceAsString());
			}
		}

	}

	@After
	public void closeClient() {
		if (client != null) {
			client.close();
		}
	}

	public static void main(String[] args) {
		ObjectMapper mapper = new ObjectMapper();
		mapper.setDateFormat(DateFormat.getDateTimeInstance(2, 2));
		List<User> users = User.getUserBuilder().genUsers(1);
		try {
			String json = mapper.writeValueAsString(users.get(0));
			System.out.println(json);
		} catch (JsonProcessingException e) {
			e.printStackTrace();
		}
	}
}
