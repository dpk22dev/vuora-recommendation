package com.intelverse.dto;

import java.util.Map;
import java.util.Set;

import org.springframework.data.annotation.Id;
import org.springframework.data.mongodb.core.mapping.Document;

@Document(collection = "rateduser")
public class RatedUser {

	@Id
	private String id;
	private Set<String> tags;
	private Object videos;
	private Set<String> users;
	private Set<String> seminars;

	public RatedUser() {
		super();
	}

	public RatedUser(String id, Set<String> tags, Object videos, Set<String> users, Set<String> seminars) {
		super();
		this.id = id;
		this.tags = tags;
		this.videos = videos;
		this.users = users;
		this.seminars = seminars;
	}

	public Set<String> getTags() {
		return tags;
	}

	public void setTags(Set<String> tags) {
		this.tags = tags;
	}

	public String getId() {
		return id;
	}

	public void setId(String id) {
		this.id = id;
	}

	public Object getVideos() {
		return videos;
	}

	public void setVideos(Object videos) {
		this.videos = videos;
	}

	public Set<String> getUsers() {
		return users;
	}

	public void setUsers(Set<String> users) {
		this.users = users;
	}

	public Set<String> getSeminars() {
		return seminars;
	}

	public void setSeminars(Set<String> seminars) {
		this.seminars = seminars;
	}

}
