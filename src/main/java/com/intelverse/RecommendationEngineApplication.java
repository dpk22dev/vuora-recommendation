package com.intelverse;

import java.net.InetSocketAddress;
import java.net.UnknownHostException;

import org.elasticsearch.client.Client;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.transport.client.PreBuiltTransportClient;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Bean;
import org.springframework.web.client.RestTemplate;

@EnableAutoConfiguration
@SpringBootApplication
public class RecommendationEngineApplication {

	@Value("${cluster.nodes}")
	private String clusterNodes;

	@Value("${cluster.name}")
	private String clusterName;

	@Bean
	public RestTemplate getTemplate() {
		return new RestTemplate();
	}

	@Bean
	public Client getClient() throws UnknownHostException {
		String[] serverAdd = clusterNodes.split(",");
		Settings settings = Settings.builder().put("cluster.name", clusterName).put("client.transport.sniff", true)
				.build();
		TransportClient client = new PreBuiltTransportClient(settings);
		for (String server : serverAdd) {
			client.addTransportAddress(new InetSocketTransportAddress(new InetSocketAddress(server, 9300)));
		}
		return client;
	}

	public static void main(String[] args) {
		SpringApplication.run(RecommendationEngineApplication.class, args);
	}
}
