package com.intelverse.controller;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import com.intelverse.service.RecommendationRunnerV2;

@RestController
@EnableAutoConfiguration
public class RecommendationController {

	@Autowired
	private RecommendationRunnerV2 runner;

	@RequestMapping("/setrecommendation")
	public void setRecommendedUser(@PathVariable(value = "user") String user) {
		runner.setUserRecommendation(user);
	}

	@RequestMapping("/getrecommendation")
	public Object getRecommendedUser(@RequestParam(value = "user") String user) {
		return runner.getUserRecommendation(user);
	}
}
