package com.intelverse.suggestion;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchHit;
import org.json.JSONArray;
import org.json.JSONObject;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import org.springframework.util.LinkedMultiValueMap;
import org.springframework.util.MultiValueMap;
import org.springframework.web.client.RestTemplate;

import com.intelverse.util.Constants;

@Service
public class VideosSuggestionV2 {

	@Autowired
	private RestTemplate restTemplate;

	private static final String THUMBIMAGE = "https://img.youtube.com/vi/%s/0.jpg";

	public Map<String, Set<VideoResponse>> getVideoSuggestions(Client client, String user, List<String> tags) {
		Map<String, Set<VideoResponse>> videoSuggestion = new HashMap<>();
		String ALL = "all";
		SearchResponse searchResponse = client.prepareSearch(Constants.ES_INDEX).setTypes(Constants.ES_EVENT_TYPE)
				.setFetchSource(new String[] { "videoId", "tags", "aTags", "description" }, null)
				.setQuery(QueryBuilders.boolQuery().should(QueryBuilders.termsQuery("tags", tags))
						.should(QueryBuilders.termsQuery("atags", tags))
						.mustNot(QueryBuilders.termQuery("requestee", user)).minimumNumberShouldMatch(1))
				.get();
		Set<VideoResponse> videoIds = new HashSet<>();
		for (SearchHit hit : searchResponse.getHits().getHits()) {
			Map<String, Object> source = hit.getSource();
			VideoResponse videoResponse = new VideoResponse();
			if (hit.getSource().get("videoid") != null) {
				videoResponse.setVideoId(source.get("videoid").toString());
			}
			List<String> tgs = (List<String>) source.get("tags");
			tgs.addAll((List<String>) source.get("aTags"));
			videoResponse.setTags(tgs);
			videoResponse.setDescription(source.get("description").toString());
			videoIds.add(videoResponse);
		}
		videoSuggestion.put(ALL, videoIds);
		if (videoIds.size() == 0) {
			videoSuggestion.putAll(getYoutubeVideoList(tags));
		}
		return videoSuggestion;
	}

	private Map<String, Set<VideoResponse>> getYoutubeVideoList(List<String> tags) {
		String url = "http://api.intelverse.com:3000/video/suggest/unauth/recommendation";
		YoutubeVideo video = new YoutubeVideo("id, snippet", tags, new ArrayList<>(), new ArrayList<>());
		String result = restTemplate.postForObject(url, video, String.class);
		Map<String, Set<VideoResponse>> vIds = getYoutubeVideoIdsFromResult(new JSONObject(result));
		return vIds;
	}

	private Map<String, Set<VideoResponse>> getYoutubeVideoIdsFromResult(JSONObject result) {
		Map<String, Set<VideoResponse>> vMap = new HashMap<>();
		result = result.getJSONObject("data");
		Set<String> keys = result.keySet();
		for (String key : keys) {
			Set<VideoResponse> vIds = new HashSet<>();
			JSONObject innerObject = result.getJSONObject(key);
			JSONArray ijArray = innerObject.getJSONArray("vidsArr");
			for (int i = 0; i < ijArray.length(); i++) {
				VideoResponse videoResponse = new VideoResponse();
				if (ijArray.getJSONObject(i).has("broadcast")) {
					String videoId = ijArray.getJSONObject(i).getJSONObject("broadcast").get("id").toString();
					videoResponse.setVideoId(videoId);
					videoResponse.setThumbImage(String.format(THUMBIMAGE, videoId));
				}
				if (ijArray.getJSONObject(i).has("snippet")) {
					videoResponse.setDescription(ijArray.getJSONObject(i).getJSONObject("snippet").getString("title"));
				}
				videoResponse.setTags(Arrays.asList(ijArray.getJSONObject(i).get("tag").toString().split(",")));
				vIds.add(videoResponse);
			}
			vMap.put(key, vIds);
		}
		return vMap;
	}

	private class YoutubeVideo {
		private String part;
		private List<String> recentlySearched;
		private List<String> recentlyWatchedVideoIds;
		private List<String> userInterestedTags;

		public YoutubeVideo(String part, List<String> recentlySearched, List<String> recentlyWatchedVideoIds,
				List<String> userInterestedTags) {
			super();
			this.part = part;
			this.recentlySearched = recentlySearched;
			this.recentlyWatchedVideoIds = recentlyWatchedVideoIds;
			this.userInterestedTags = userInterestedTags;
		}

		public String getPart() {
			return part;
		}

		public void setPart(String part) {
			this.part = part;
		}

		public List<String> getRecentlySearched() {
			return recentlySearched;
		}

		public void setRecentlySearched(List<String> recentlySearched) {
			this.recentlySearched = recentlySearched;
		}

		public List<String> getRecentlyWatchedVideoIds() {
			return recentlyWatchedVideoIds;
		}

		public void setRecentlyWatchedVideoIds(List<String> recentlyWatchedVideoIds) {
			this.recentlyWatchedVideoIds = recentlyWatchedVideoIds;
		}

		public List<String> getUserInterestedTags() {
			return userInterestedTags;
		}

		public void setUserInterestedTags(List<String> userInterestedTags) {
			this.userInterestedTags = userInterestedTags;
		}

	}

	private static class VideoResponse {
		private String videoId;
		private List<String> tags;
		private String description;
		private String thumbImage;

		public String getThumbImage() {
			return thumbImage;
		}

		public void setThumbImage(String thumbImage) {
			this.thumbImage = thumbImage;
		}

		public String getVideoId() {
			return videoId;
		}

		public void setVideoId(String videoId) {
			this.videoId = videoId;
		}

		public List<String> getTags() {
			return tags;
		}

		public void setTags(List<String> tags) {
			this.tags = tags;
		}

		public String getDescription() {
			return description;
		}

		public void setDescription(String description) {
			this.description = description;
		}

	}
}
