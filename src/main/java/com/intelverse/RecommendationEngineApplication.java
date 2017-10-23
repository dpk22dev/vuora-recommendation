package com.intelverse;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.List;

import org.elasticsearch.client.Client;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.transport.client.PreBuiltTransportClient;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Bean;

import com.mongodb.DB;
import com.mongodb.MongoClient;
import com.mongodb.MongoClientOptions;
import com.mongodb.ServerAddress;
import com.mongodb.WriteConcern;

@SpringBootApplication
public class RecommendationEngineApplication {

	@Value("${cluster.nodes}")
	private String clusterNodes;

	@Value("${cluster.name}")
	private String clusterName;

	@Bean
	public Client getClient() throws UnknownHostException {
		String[] serverAdd = clusterNodes.split(",");
		Settings settings = Settings.builder().put("cluster.name", clusterName).build();
		TransportClient client = new PreBuiltTransportClient(settings);
		for (String server : serverAdd) {
			client.addTransportAddress(new InetSocketTransportAddress(InetAddress.getByName(server), 9300));
		}
		return client;
	}

	public static void main(String[] args) {
		SpringApplication.run(RecommendationEngineApplication.class, args);
	}
}
