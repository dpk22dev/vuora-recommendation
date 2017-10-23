package com.intelverse.suggestion;

import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchHit;

import com.intelverse.util.Constants;
import com.mongodb.DB;

public class UserSuggestionsV2 {

	public Set<String> getUserSuggestions(Client client, String user, List<String> tags) {
		Set<String> userSuggestions = new HashSet<>();
		SearchResponse searchResponse = client.prepareSearch(Constants.ES_INDEX).setTypes(Constants.ES_USERS_TYPE)
				.setQuery(QueryBuilders.termsQuery("tags", tags)).setSize(100).get();
		for (SearchHit hit : searchResponse.getHits().getHits()) {
			String uid = hit.getSource().get("id").toString();
			userSuggestions.add(uid);
		}
		userSuggestions.remove(user);
		return userSuggestions;
	}
}
