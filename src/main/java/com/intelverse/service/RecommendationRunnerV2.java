package com.intelverse.service;

import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.search.Scroll;
import org.elasticsearch.search.SearchHit;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.scheduling.annotation.EnableScheduling;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;

import com.intelverse.dto.RatedUser;
import com.intelverse.repository.RatedUserRepository;
import com.intelverse.suggestion.SeminarSuggestionsV2;
import com.intelverse.suggestion.UserSuggestionsV2;
import com.intelverse.suggestion.VideosSuggestionV2;
import com.intelverse.util.Constants;

@EnableScheduling
@Service
public class RecommendationRunnerV2 {

	@Autowired
	private Client client;

	@Autowired
	private RatedUserRepository userRepository;

	private static Logger logger = Logger.getLogger(RecommendationRunnerV2.class.getName());

	public RatedUser getUserRecommendation(String user) {
		logger.log(Level.INFO, "getting user recomm for user :" + user);
		RatedUser ratedUser = userRepository.findOne(user);
		if (ratedUser == null) {
			return setUserRecommendation(user);
		}
		return ratedUser;

	}

	public RatedUser setUserRecommendation(String user) {
		logger.log(Level.INFO, "setting user recomm for user :" + user);
		SeminarSuggestionsV2 seminarSuggestionsV2 = new SeminarSuggestionsV2();
		VideosSuggestionV2 videosSuggestionV2 = new VideosSuggestionV2();
		UserSuggestionsV2 userSuggestionsV2 = new UserSuggestionsV2();
		RatedUser ratedUser = null;
		GetResponse searchResp = client.prepareGet(Constants.ES_INDEX, Constants.ES_USERS_TYPE, user).get();
		if (searchResp.isExists()) {
			List<String> tags = (List<String>) searchResp.getSource().get("tags");
			Set<String> seminars = seminarSuggestionsV2.getSeminarSuggestions(client, user, tags);
			Set<String> users = userSuggestionsV2.getUserSuggestions(client, user, tags);
			Set<String> vidoes = videosSuggestionV2.getVideoSuggestions(client, user, tags);

			ratedUser = new RatedUser(user, new HashSet<>(tags), vidoes, vidoes, seminars);
			userRepository.save(ratedUser);
		}
		return ratedUser;

	}

	@Scheduled(fixedDelay = 86400000)
	private void recommendation() {
		logger.log(Level.INFO, "starting user recomm thread ");
		SeminarSuggestionsV2 seminarSuggestionsV2 = new SeminarSuggestionsV2();
		VideosSuggestionV2 videosSuggestionV2 = new VideosSuggestionV2();
		UserSuggestionsV2 userSuggestionsV2 = new UserSuggestionsV2();
		SearchResponse scrollResp = client.prepareSearch(Constants.ES_INDEX).setTypes(Constants.ES_USERS_TYPE)
				.setScroll(new Scroll(new TimeValue(60000))).setSize(1000).get();
		do {
			for (SearchHit hit : scrollResp.getHits().getHits()) {
				Map<String, Object> source = hit.getSource();
				String user = source.get("id").toString();
				List<String> tags = (List<String>) source.get("tags");
				Set<String> seminars = seminarSuggestionsV2.getSeminarSuggestions(client, user, tags);
				Set<String> users = userSuggestionsV2.getUserSuggestions(client, user, tags);
				Set<String> vidoes = videosSuggestionV2.getVideoSuggestions(client, user, tags);
				RatedUser ratedUser = new RatedUser(user, new HashSet<>(tags), vidoes, vidoes, seminars);
				userRepository.save(ratedUser);
			}
			scrollResp = client.prepareSearchScroll(scrollResp.getScrollId()).setScroll(new TimeValue(60000)).execute()
					.actionGet();
		} while (scrollResp.getHits().getHits().length != 0);
	}

}
