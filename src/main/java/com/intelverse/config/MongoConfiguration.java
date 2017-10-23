package com.intelverse.config;

import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Lazy;
import org.springframework.data.mongodb.MongoDbFactory;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.data.mongodb.core.SimpleMongoDbFactory;

import com.mongodb.MongoClient;
import com.mongodb.MongoClientOptions;
import com.mongodb.ServerAddress;
import com.mongodb.WriteConcern;
import com.mongodb.client.MongoDatabase;

@Configuration
public class MongoConfiguration {

	private static final Logger LOG = LoggerFactory.getLogger(MongoConfiguration.class);

	@Value(value = "${mongodb.server.seeds:null}")
	private String host;

	@Value(value = "${mongodb.server.port:null}")
	private Integer port;

	@Value(value = "${mongodb.database.name:null}")
	private String db;

	@Value(value = "${mongodb.socket.timeout:15000}")
	private Integer socketTimeout;

	@Value(value = "${mongodb.connect.timeout:1000}")
	private Integer connectTimeout;

	@Value(value = "${mongodb.max.wait.time:1000}")
	private Integer maxWaitTimeout;

	@Bean
	public MongoClient client() throws UnknownHostException {
		List<ServerAddress> seeds = new ArrayList<>();
		String arr[] = host.split(",");
		for (String ip : arr)
			seeds.add(new ServerAddress(ip, port));
		LOG.info("Connecting to mongo db server.");
		MongoClient client = new MongoClient(seeds,
				new MongoClientOptions.Builder().socketTimeout(socketTimeout).connectTimeout(connectTimeout)
						.maxWaitTime(maxWaitTimeout).writeConcern(WriteConcern.JOURNALED).build());
		LOG.info("Creating Mongodb client bean");
		return client;
	}

	@Bean
	public MongoDbFactory mongoDbFactory() throws UnknownHostException {
		SimpleMongoDbFactory simpleMongoDbFactory = new SimpleMongoDbFactory(client(), db);
		LOG.info("Creating Mongodb factory bean");
		return simpleMongoDbFactory;
	}

	@Bean
	public MongoTemplate mongoTemplate() throws Exception {
		LOG.info("Creating Mongodb template bean");
		return new MongoTemplate(mongoDbFactory());
	}

	@Bean
	public MongoDatabase getMongoDB() throws UnknownHostException {
		MongoDatabase database = client().getDatabase(db);
		return database;
	}
}