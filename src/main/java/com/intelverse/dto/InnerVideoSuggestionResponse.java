package com.intelverse.dto;

import java.util.List;
import java.util.Set;

public class InnerVideoSuggestionResponse {

	private String key;
	private Set<String> values;

	public InnerVideoSuggestionResponse() {
		super();
	}

	public InnerVideoSuggestionResponse(String key, Set<String> values) {
		super();
		this.key = key;
		this.values = values;
	}

	public String getKey() {
		return key;
	}

	public void setKey(String key) {
		this.key = key;
	}

	public Set<String> getValues() {
		return values;
	}

	public void setValues(Set<String> values) {
		this.values = values;
	}

}
