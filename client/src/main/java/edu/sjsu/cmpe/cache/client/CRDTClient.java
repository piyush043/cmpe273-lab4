package edu.sjsu.cmpe.cache.client;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Future;

import com.mashape.unirest.http.HttpResponse;
import com.mashape.unirest.http.JsonNode;
import com.mashape.unirest.http.Unirest;
import com.mashape.unirest.http.async.Callback;
import com.mashape.unirest.http.exceptions.UnirestException;

public class CRDTClient {
	private static List<DistributedCacheService> cacheServers = new ArrayList<DistributedCacheService>();
	private CountDownLatch countDownLatch;

	public CRDTClient() {
		this.cacheServers = new ArrayList<DistributedCacheService>();
		cacheServers.add(new DistributedCacheService("http://localhost:3000"));
		cacheServers.add(new DistributedCacheService("http://localhost:3001"));
		cacheServers.add(new DistributedCacheService("http://localhost:3002"));
	}

	public boolean put(long key, String value) {
		try {
			final ArrayList<DistributedCacheService> putSuccessServerList = new ArrayList<DistributedCacheService>();
			this.countDownLatch = new CountDownLatch(cacheServers.size());
			for (final DistributedCacheService currentCacheServer : cacheServers) {
				Future<HttpResponse<JsonNode>> future = Unirest
						.put(currentCacheServer.getServerUrl()
								+ "/cache/{key}/{value}")
						.header("accept", "application/json")
						.routeParam("key", Long.toString(key))
						.routeParam("value", value)
						.asJsonAsync(new Callback<JsonNode>() {

							public void failed(UnirestException e) {
								System.out
										.println("The request has failed on : "
												+ currentCacheServer
														.getServerUrl());
								countDownLatch.countDown();
							}

							public void completed(
									HttpResponse<JsonNode> response) {
								putSuccessServerList.add(currentCacheServer);
								System.out
										.println("The request is successfull on : "
												+ currentCacheServer
														.getServerUrl());
								countDownLatch.countDown();
							}

							public void cancelled() {
								System.out
										.println("The request has been cancelled");
								countDownLatch.countDown();
							}

						});
			}
			this.countDownLatch.await();
			System.out.println("Put request successfull on "
					+ putSuccessServerList.size() + " servers");
			if (putSuccessServerList.size() > 1) {
				System.out.println("");
				return true;
			} else {
				System.out
						.println("Removing keys as number of successfull servers are less.");
				this.countDownLatch = new CountDownLatch(
						putSuccessServerList.size());
				for (final DistributedCacheService currentCacheServer : putSuccessServerList) {
					currentCacheServer.remove(key);
				}
				this.countDownLatch.await();
				Unirest.shutdown();
				return false;
			}
		} catch (Exception e) {
			System.err.println(e);
			return false;
		}
	}

	public String get(long key) throws UnirestException, InterruptedException, IOException {
		final Map<DistributedCacheService, String> resultMap = new HashMap<DistributedCacheService, String>();
		this.countDownLatch = new CountDownLatch(cacheServers.size());
		for (final DistributedCacheService currentCacheServer : cacheServers) {
			Future<HttpResponse<JsonNode>> future = Unirest
					.get(currentCacheServer.getServerUrl() + "/cache/{key}")
					.header("accept", "application/json")
					.routeParam("key", Long.toString(key))
					.asJsonAsync(new Callback<JsonNode>() {

						public void failed(UnirestException e) {
							System.out.println("The request has failed on : "
									+ currentCacheServer.getServerUrl());
							countDownLatch.countDown();
						}

						public void completed(HttpResponse<JsonNode> response) {
							resultMap.put(currentCacheServer, response
									.getBody().getObject().getString("value"));
							System.out
									.println("The request is successfull on : "
											+ currentCacheServer.getServerUrl());
							countDownLatch.countDown();
						}

						public void cancelled() {
							System.out
									.println("The request has been cancelled");
							countDownLatch.countDown();
						}

					});
		}
		
		this.countDownLatch.await();
		final Map<String, Integer> countMap = new HashMap<String, Integer>();
		int maxCount = 0;
		for (String value : resultMap.values()) {
			int count = 1;
			if (countMap.containsKey(value)) {
				count = countMap.get(value);
				count++;
			}
			if (maxCount < count)
				maxCount = count;
			countMap.put(value, count);
		}

		String value = this.getKeyFromValue(countMap, maxCount);

		if (maxCount != this.cacheServers.size()) {
			for (Map.Entry<DistributedCacheService, String> cacheServerData : resultMap
					.entrySet()) {
				if (!value.equals(cacheServerData.getValue())) {
					System.out.println("Repairing " + cacheServerData.getKey());
					HttpResponse<JsonNode> response = Unirest
							.put(cacheServerData.getKey()
									+ "/cache/{key}/{value}")
							.header("accept", "application/json")
							.routeParam("key", Long.toString(key))
							.routeParam("value", value).asJson();
				}
			}
			for (DistributedCacheService cacheServer : this.cacheServers) {
				if (resultMap.containsKey(cacheServer))
					continue;
				System.out.println("Repairing " + cacheServer.getServerUrl());
				HttpResponse<JsonNode> response = Unirest
						.put(cacheServer.getServerUrl()
								+ "/cache/{key}/{value}")
						.header("accept", "application/json")
						.routeParam("key", Long.toString(key))
						.routeParam("value", value).asJson();
			}
		} else {
			System.out.println("Repair not needed");
		}
		System.out.println("Exiting Unirest services..");
		Unirest.shutdown();
		return value;

	}

	public String getKeyFromValue(Map<String, Integer> map, int value) {
		for (Map.Entry<String, Integer> entry : map.entrySet()) {
			if (value == entry.getValue())
				return entry.getKey();
		}
		return null;
	}
}
