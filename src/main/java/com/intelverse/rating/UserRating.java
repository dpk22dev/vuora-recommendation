package com.intelverse.rating;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import org.apache.logging.log4j.core.util.KeyValuePair;
import org.elasticsearch.action.search.MultiSearchResponse;
import org.elasticsearch.action.search.MultiSearchResponse.Item;
import org.elasticsearch.action.search.SearchRequestBuilder;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.sort.SortOrder;

import com.google.common.collect.Lists;
import com.intelverse.util.Constants;
import com.mongodb.BasicDBObject;
import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.DBCursor;
import com.mongodb.DBObject;

public class UserRating {

	private static int THREADS;

	public UserRating(int threads) {
		THREADS = threads;
	}

	private Double rateUser(Client client, DB db, String user) {
		Map<String, Double> wMap = weightMap();
		ExecutorService executorService = Executors.newFixedThreadPool(THREADS);
		DBCollection collection = db.getCollection(Constants.MONGO_COLL_TAGS);
		DBObject query = new BasicDBObject("id", user);
		DBCursor cursor = collection.find(query);
		double tagValue = 0;
		double totalTag = 0;
		while (cursor.hasNext()) {
			DBObject object = cursor.next();
			String tag = object.get("tag").toString();
			List<Future<KeyValuePair>> resultThreads = new ArrayList<>();
			Callable<KeyValuePair> byLikesOnVideos = () -> {
				return byLikesOnVideos(db, client, user, tag);
			};

			Callable<KeyValuePair> byVideosCreatedByUser = () -> {
				return byVideosCreatedByUser(client, user, tag);
			};

			Callable<KeyValuePair> bySearchOnSkills = () -> {
				return bySearchOnSkills(db, client, user, tag);
			};

			Callable<KeyValuePair> byVideosWatchedOnSkill = () -> {
				return byVideosWatchedOnSkill(client, user, tag);
			};

			Callable<KeyValuePair> bySeminarAttended = () -> {
				return bySeminarAttended(client, user, tag);
			};

			Callable<KeyValuePair> byConferenceAccepted = () -> {
				return byConferenceAccepted(db, client, user, tag);
			};

			Callable<KeyValuePair> bySeminarRequesting = () -> {
				return bySeminarRequesting(db, client, user, tag);
			};
			resultThreads.add(executorService.submit(byLikesOnVideos));
			resultThreads.add(executorService.submit(byVideosCreatedByUser));
			resultThreads.add(executorService.submit(bySearchOnSkills));
			resultThreads.add(executorService.submit(byVideosWatchedOnSkill));
			resultThreads.add(executorService.submit(bySeminarAttended));
			resultThreads.add(executorService.submit(byConferenceAccepted));
			resultThreads.add(executorService.submit(bySeminarRequesting));
			double rate = 0.0;
			for (Future<KeyValuePair> resultThread : resultThreads) {
				try {
					KeyValuePair valuePair = resultThread.get();
					String key = valuePair.getKey();
					if (wMap.containsKey(key)) {
						rate += rate * Double.valueOf(valuePair.getValue());
						tagValue += rate;
						totalTag++;
					} else {
						rate += 0;
					}
				} catch (InterruptedException | ExecutionException e) {
					e.printStackTrace();
				}
			}
			BasicDBObject updateQuery = new BasicDBObject();
			updateQuery.append("$set", new BasicDBObject().append("rate", rate));

			BasicDBObject searchQuery = new BasicDBObject();
			searchQuery.append("id", user).append("tag", tag);
			collection.updateMulti(searchQuery, updateQuery);
		}
		collection = db.getCollection(Constants.MONGO_COLL_RUSER);
		BasicDBObject updateQuery = new BasicDBObject();
		double finalRate = totalTag == 0 ? 1 : tagValue / totalTag;
		updateQuery.append("$set", new BasicDBObject().append("rate", finalRate));

		BasicDBObject searchQuery = new BasicDBObject();
		searchQuery.append("id", user);
		collection.updateMulti(searchQuery, updateQuery);
		return tagValue;
	}

	private KeyValuePair byLikesOnVideos(DB db, Client client, String user, String skill) {
		long upvote = 0;
		long totalVote = 0;
		Set<String> midList = new HashSet<>();
		SearchResponse searchResponse = client.prepareSearch(Constants.ES_INDEX).setTypes(Constants.ES_EVENT_TYPE)
				.setScroll(new TimeValue(60000))
				.setQuery(QueryBuilders.boolQuery().must(QueryBuilders.termQuery("mType", "SEMINAR"))
						.must(QueryBuilders.termQuery("atags", skill)).must(QueryBuilders.termQuery("requestee", user))
						.must(QueryBuilders.termQuery("state", new String[] { "ACCEPTED", "PARTIALLY_ACCEPTED" })))
				.setSize(1000).get();
		do {
			for (SearchHit hit : searchResponse.getHits().getHits()) {
				midList.add(hit.fields().get("mid").getValue().toString());
			}
			searchResponse = client.prepareSearchScroll(searchResponse.getScrollId()).setScroll(new TimeValue(60000))
					.execute().actionGet();
		} while (searchResponse.getHits().getHits().length != 0);

		List<List<String>> partMids = Lists.partition(new ArrayList<>(midList), 1000);
		for (List<String> pMids : partMids) {
			DBCollection collection = db.getCollection(Constants.MONGO_COLL_VIDEOS);
			DBObject query = new BasicDBObject();
			query.put("mid", new BasicDBObject("$in", pMids));
			DBCursor cursor = collection.find(query);
			while (cursor.hasNext()) {
				DBObject object = cursor.next();
				long upvt = Long.valueOf(object.get("upvote").toString());
				long dwnvt = Long.valueOf(object.get("downvote").toString());
				upvote += upvt;
				totalVote += upvt + dwnvt;
			}
		}
		totalVote = totalVote == 0 ? 1 : totalVote;
		return new KeyValuePair("V1", Double.toString(upvote / (double) totalVote));
	}

	private KeyValuePair byVideosCreatedByUser(Client client, String user, String skill) {

		SearchResponse searchResponse = client.prepareSearch(Constants.ES_INDEX).setTypes(Constants.ES_EVENT_TYPE)
				.setQuery(QueryBuilders.boolQuery().must(QueryBuilders.termQuery("mType", "SEMINAR"))
						.must(QueryBuilders.termQuery("requestee", user)).must(QueryBuilders.termQuery("atags", skill))
						.must(QueryBuilders.termQuery("state", new String[] { "ACCEPTED", "PARTIALLY_ACCEPTED" })))
				.setSize(0).get();
		long skillVideos = searchResponse.getHits().getTotalHits();
		searchResponse = client.prepareSearch(Constants.ES_INDEX).setTypes(Constants.ES_EVENT_TYPE)
				.setQuery(QueryBuilders.boolQuery().must(QueryBuilders.termQuery("requestee", user))
						.must(QueryBuilders.termQuery("mType", "SEMINAR")).must(
								QueryBuilders.termQuery("state", new String[] { "ACCEPTED", "PARTIALLY_ACCEPTED" })))
				.setSize(0).get();
		long allVideos = searchResponse.getHits().getTotalHits() == 0 ? 1 : searchResponse.getHits().getTotalHits();
		return new KeyValuePair("SK1", Double.toString(skillVideos / (double) allVideos));
	}

	private KeyValuePair bySearchOnSkills(DB db, Client client, String user, String skill) {
		long searchOnSkill = 0;
		long totalTagSearch = 0;
		SearchRequestBuilder totalSearch = client.prepareSearch(Constants.ES_INDEX).setTypes(Constants.ES_ACTIVITY_TYPE)
				.setQuery(QueryBuilders.boolQuery().must(QueryBuilders.termQuery("event", "tag_search"))
						.must(QueryBuilders.termQuery("user", user)))
				.setSize(0);

		SearchRequestBuilder tagSearch = client.prepareSearch(Constants.ES_INDEX).setTypes(Constants.ES_ACTIVITY_TYPE)
				.setQuery(QueryBuilders.boolQuery().must(QueryBuilders.termQuery("event", "tag_search"))
						.must(QueryBuilders.termQuery("user", user)).must(QueryBuilders.termQuery("tag", skill)))
				.setSize(0);
		MultiSearchResponse multiSearchResponse = client.prepareMultiSearch().add(totalSearch).add(tagSearch).get();
		Item[] itemResponse = multiSearchResponse.getResponses();
		searchOnSkill = itemResponse[0].getResponse().getHits().getTotalHits();
		totalTagSearch = itemResponse[1].getResponse().getHits().getTotalHits();

		return new KeyValuePair("SK2", Double.toString(searchOnSkill / (double) totalTagSearch));
	}

	private KeyValuePair byVideosWatchedOnSkill(Client client, String user, String skill) {
		Map<String, Long> watchedDuration = new HashMap<String, Long>();
		Map<String, Long> totalDuration = new HashMap<String, Long>();
		long skillVideoCount = 0;
		long totalVideoCount = totalDuration.size() == 0 ? totalDuration.size() : 1;
		long skillVideoDuration = 0;
		long totalVideoDuration = 0;
		SearchResponse searchResponse = client.prepareSearch(Constants.ES_INDEX).setTypes(Constants.ES_ACTIVITY_TYPE)
				.setScroll(new TimeValue(60000))
				.setQuery(QueryBuilders.boolQuery().must(QueryBuilders.termQuery("user", user))
						.must(QueryBuilders.termQuery("event", "video_view")))
				.setSize(1000).get();
		do {
			for (SearchHit hit : searchResponse.getHits().getHits()) {
				long wDuration = Long.valueOf(hit.fields().get("viewTime").getValue().toString());
				long tDuration = Long.valueOf(hit.fields().get("totalTime").getValue().toString());
				String vId = hit.fields().get("tag").toString();
				if (watchedDuration.containsKey(vId)) {
					wDuration = watchedDuration.get(vId) > wDuration ? watchedDuration.get(vId) : wDuration;
				}
				if (totalDuration.containsKey(vId)) {
					tDuration = totalDuration.get(vId) > tDuration ? totalDuration.get(vId) : tDuration;
				}
				watchedDuration.put(vId, wDuration);
				totalDuration.put(vId, tDuration);
			}
			searchResponse = client.prepareSearchScroll(searchResponse.getScrollId()).setScroll(new TimeValue(60000))
					.execute().actionGet();
		} while (searchResponse.getHits().getHits().length != 0);
		Set<String> vids = totalDuration.keySet();
		List<List<String>> partVids = Lists.partition(new ArrayList<>(vids), 1000);
		for (List<String> pVids : partVids) {
			searchResponse = client.prepareSearch(Constants.ES_INDEX).setTypes(Constants.ES_EVENT_TYPE)
					.setFetchSource("videoId", null)
					.setQuery(QueryBuilders.boolQuery().must(QueryBuilders.termQuery("atags", skill))
							.must(QueryBuilders.termsQuery("videoId", pVids)))
					.addSort("timestamp", SortOrder.DESC).setSize(0).get();
			skillVideoCount += searchResponse.getHits().getTotalHits();
		}
		skillVideoDuration = watchedDuration.values().stream().mapToLong(Long::longValue).sum();
		totalVideoDuration = totalDuration.values().stream().mapToLong(Long::longValue).sum();
		totalVideoDuration = totalVideoDuration == 0 ? 1 : totalVideoDuration;
		return new KeyValuePair("SK3", Double.toString(
				((skillVideoCount / (double) totalVideoCount) + (skillVideoDuration / (double) totalVideoDuration))));
	}

	private KeyValuePair bySeminarAttended(Client client, String user, String skill) {
		long seminarByTagVal = 0;
		long totalSeminarVal = 0;
		SearchRequestBuilder seminarByTag = client.prepareSearch(Constants.ES_INDEX).setTypes(Constants.ES_EVENT_TYPE)
				.setQuery(QueryBuilders.boolQuery().must(QueryBuilders.termQuery("requestee", user))
						.must(QueryBuilders.termQuery("mType", "SEMINAR")).must(QueryBuilders.termQuery("tags", skill)))
				.setSize(0);

		SearchRequestBuilder totalSeminar = client.prepareSearch(Constants.ES_INDEX).setTypes(Constants.ES_EVENT_TYPE)
				.setQuery(QueryBuilders.boolQuery().must(QueryBuilders.termQuery("requestee", user))
						.must(QueryBuilders.termQuery("mType", "SEMINAR")))
				.setSize(0);
		MultiSearchResponse multiSearchResponse = client.prepareMultiSearch().add(seminarByTag).add(totalSeminar).get();
		Item[] itemResponse = multiSearchResponse.getResponses();
		seminarByTagVal = itemResponse[0].getResponse().getHits().getTotalHits();
		totalSeminarVal = itemResponse[1].getResponse().getHits().getTotalHits();
		return new KeyValuePair("SK4", Double.toString(seminarByTagVal / (double) totalSeminarVal));
	}

	private KeyValuePair byConferenceAccepted(DB db, Client client, String user, String skill) {
		Set<String> requestee = new HashSet<>();
		long totalConfAccepted = 0;
		long totalRequested = 0;
		SearchResponse searchResponse = client.prepareSearch(Constants.ES_INDEX).setTypes(Constants.ES_EVENT_TYPE)
				.setQuery(QueryBuilders.boolQuery().must(QueryBuilders.termQuery("requestor", user))
						.must(QueryBuilders.termQuery("tags", skill))
						.must(QueryBuilders.termQuery("mType", "CONFERENCE"))
						.must(QueryBuilders.termQuery("state", "ACCEPTED")))
				.get();
		for (SearchHit searchHit : searchResponse.getHits().getHits()) {
			totalConfAccepted++;
			requestee.add(searchHit.getSource().get("requestee").toString());
		}
		searchResponse = client.prepareSearch(Constants.ES_INDEX).setTypes(Constants.ES_EVENT_TYPE)
				.setQuery(QueryBuilders.boolQuery().must(QueryBuilders.termQuery("requestor", user))
						.must(QueryBuilders.termQuery("mType", "CONFERENCE"))
						.must(QueryBuilders.termQuery("tags", skill)))
				.setSize(0).get();
		totalRequested = searchResponse.getHits().getTotalHits();

		// TO-DO need to revisit to cover case if requestee count is more tha
		// 1000
		long totalRating = 0;
		long totalCount = 0;
		DBCollection collection = db.getCollection(Constants.MONGO_COLL_RUSER);
		DBObject query = new BasicDBObject("tags.name", skill);
		query.put("id", new BasicDBObject("$in", requestee));
		DBCursor cursor = collection.find(query);
		while (cursor.hasNext()) {
			DBObject object = cursor.next();
			String rate = object.get("tags.name") != null ? object.get("tags.name").toString() : "0";
			totalRating += Long.valueOf(rate);
			totalCount++;
		}

		double avgRating = totalRating / (double) totalCount;
		double fVal = totalRequested == 0 ? 0 : (totalConfAccepted / (double) totalRequested) * avgRating;
		return new KeyValuePair("F1", Double.toString(fVal));

	}

	private KeyValuePair bySeminarRequesting(DB db, Client client, String user, String skill) {
		Set<String> requestee = new HashSet<>();
		long totalSemRequested = 0;
		SearchResponse searchResponse = client.prepareSearch(Constants.ES_INDEX).setTypes(Constants.ES_EVENT_TYPE)
				.setQuery(QueryBuilders.boolQuery().must(QueryBuilders.termQuery("requestor", user))
						.must(QueryBuilders.termQuery("tags", skill)).must(QueryBuilders.termQuery("mType", "SEMINAR")))
				.get();
		for (SearchHit searchHit : searchResponse.getHits().getHits()) {
			totalSemRequested++;
			requestee.add(searchHit.getSource().get("requestee").toString());
		}

		// TO-DO need to revisit to cover case if requestee count is more than
		// 1000
		long totalRating = 0;
		long totalCount = 0;
		DBCollection collection = db.getCollection(Constants.MONGO_COLL_RUSER);
		DBObject query = new BasicDBObject("tags.name", skill);
		query.put("id", new BasicDBObject("$in", requestee));
		DBCursor cursor = collection.find(query);
		while (cursor.hasNext()) {
			DBObject object = cursor.next();
			String rate = object.get("tags.name") != null ? object.get("tags.name").toString() : "0";
			totalRating += Long.valueOf(rate);
			totalCount++;
		}

		double avgRating = totalRating / (double) totalCount;
		double fVal = totalSemRequested == 0 ? 0 : (totalSemRequested / (double) totalSemRequested) * avgRating;
		return new KeyValuePair("SK5", Double.toString(fVal));
	}

	private Map<String, Double> weightMap() {
		Map<String, Double> weightedMap = new HashMap<>();
		weightedMap.put("V1", 0.1);
		weightedMap.put("SK1", 0.1);
		weightedMap.put("SK2", 0.1);
		weightedMap.put("SK3", 0.1);
		weightedMap.put("SK4", 0.1);
		weightedMap.put("SK5", 0.1);
		weightedMap.put("F1", 0.1);
		return weightedMap;
	}
}
