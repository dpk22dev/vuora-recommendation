package com.intelverse.suggestion;

import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchHit;

import com.intelverse.util.Constants;

public class VideosSuggestionV2 {

	public Set<String> getVideoSuggestions(Client client, String user, List<String> tags) {
		Set<String> videoSuggestions = new HashSet<>();
		SearchResponse searchResponse = client.prepareSearch(Constants.ES_INDEX).setTypes(Constants.ES_EVENT_TYPE)
				.setFetchSource("videoid", null).setQuery(QueryBuilders.termsQuery("tags", tags)).get();
		for (SearchHit hit : searchResponse.getHits().getHits()) {
			String videoId = hit.getSource().get("videoid").toString();
			videoSuggestions.add(videoId);
		}
		return videoSuggestions;
	}
}
