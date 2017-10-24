package com.intelverse.suggestion;

import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchHit;

import com.intelverse.util.Constants;

public class SeminarSuggestionsV2 {

	public Set<String> getSeminarSuggestions(Client client, String user, List<String> tags) {
		Set<String> seminarSuggestions = new HashSet<>();
		SearchResponse searchResponse = client.prepareSearch(Constants.ES_INDEX).setTypes(Constants.ES_EVENT_TYPE)
				.setFetchSource("id", null)
				.setQuery(QueryBuilders.boolQuery().must(QueryBuilders.termsQuery("tags", tags))
						.mustNot(QueryBuilders.termQuery("requestee", user)))
				.setSize(100).get();
		for (SearchHit hit : searchResponse.getHits().getHits()) {
			if (hit.getSource().get("id") != null) {
				String seminarId = hit.getSource().get("id").toString();
				seminarSuggestions.add(seminarId);
			}
		}
		return seminarSuggestions;
	}
}
