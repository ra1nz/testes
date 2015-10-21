package net.zsy.testes;

import java.io.IOException;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

import org.elasticsearch.action.bulk.BulkItemResponse;
import org.elasticsearch.action.bulk.BulkProcessor;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.delete.DeleteResponse;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.get.MultiGetItemResponse;
import org.elasticsearch.action.get.MultiGetResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexRequestBuilder;
import org.elasticsearch.action.index.IndexResponse;
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
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;
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

	/*
	 * ===================================Document API
	 * Begin=======================================
	 */
	/**
	 * 使用JAVABEAN创建索引
	 */
	@Test
	public void createIndexUseBean() {
		List<User> users = User.getUserBuilder().genUsers(10000);
		ObjectMapper mapper = new ObjectMapper();

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
	 * 使用XContentBuilder创建索引
	 */
	@Test
	public void createIndexUseHelper() {
		List<User> users = User.getUserBuilder().genUsers(10);
		try {
			for (User user : users) {
				XContentBuilder contentbuilder = XContentFactory.jsonBuilder();
				IndexRequestBuilder builder = client
						.prepareIndex("indices", User.TYPE)
						.setId(String.valueOf(user.getId()))
						.setSource(
								contentbuilder.startObject().field("id", user.getId()).field("name", user.getName())
										.field("age", user.getAge()).field("gender", user.getGender())
										.field("birthday", user.getBirthday().getTime())
										.field("mobile", user.getMobile()).field("hobbies", user.getHobbies())
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
	public void getIndex() {
		GetResponse response = client.prepareGet("indices", User.TYPE, "597421").setOperationThreaded(false).get();
		System.out.println(response.getSourceAsMap());
	}

	/**
	 * 根据ID删除一个索引文档
	 */
	@Test
	public void deleteIndex() {
		DeleteResponse response = client.prepareDelete("indices", User.TYPE, "597421").get();
		System.out.println(response.getContext());
	}

	/**
	 * 更新索引（不存在即创建的写法）
	 */
	@Test
	public void updateIndex() {

		User u = User.getUserBuilder().genUsers(1).get(0);
		ObjectMapper mapper = new ObjectMapper();
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
	public void multiGetIndex() {
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
	public void bulkRequest() {
		User u = User.getUserBuilder().genUsers(1).get(0);
		ObjectMapper mapper = new ObjectMapper();
		try {
			BulkResponse response = client
					.prepareBulk()
					.add(client.prepareIndex("indices", User.TYPE, String.valueOf(u.getId())).setSource(
							mapper.writeValueAsBytes(u))).add(client.prepareDelete("indices", User.TYPE, "837407"))
					.get();
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
	public void bulkProcessor() {
		BulkProcessor bulkProcessor = BulkProcessor
				.builder(client, new BulkProcessor.Listener() {

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
		try {
			bulkProcessor.add(new IndexRequest("indices", User.TYPE, String.valueOf(u.getId())).source(mapper
					.writeValueAsBytes(u)));
		} catch (JsonProcessingException e) {
			e.printStackTrace();
		}
		try {
			bulkProcessor.awaitClose(1, TimeUnit.MINUTES);
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
	}

	/*
	 * ===================================Document API
	 * End=======================================
	 */

	/*
	 * ===================================Search API
	 * Begin=======================================
	 */
	@Test
	public void searchIndex() {
		// 查询全部
		// client.prepareSearch().get();
		SearchRequestBuilder searchRequestBuilder = client
				.prepareSearch("indices")
				.setTypes(User.TYPE)
				.setSearchType(SearchType.QUERY_THEN_FETCH)
				.setQuery(
						QueryBuilders.boolQuery().must(QueryBuilders.matchPhraseQuery("name", "Monkey D Luffy"))
								.must(QueryBuilders.matchPhraseQuery("gender", "male")))
				.setPostFilter(QueryBuilders.rangeQuery("age").from(0).to(20)).setExplain(true).setFrom(0).setSize(3);

		System.out.println(searchRequestBuilder.toString().replaceAll("\\s", ""));
		SearchResponse response = searchRequestBuilder.get();
		SearchHits searchHits = response.getHits();
		for (Iterator<SearchHit> it = searchHits.iterator(); it.hasNext();) {
			SearchHit sh = it.next();
			System.out.println(sh.getSourceAsString());
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
		List<User> users = User.getUserBuilder().genUsers(1);
		try {
			String json = mapper.writeValueAsString(users.get(0));
			System.out.println(json);
		} catch (JsonProcessingException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
}
