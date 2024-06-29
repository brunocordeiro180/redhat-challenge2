package br.com.redhat.streamer.config;

import org.apache.http.HttpHost;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestHighLevelClient;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class ElasticSearchConfig {

    @Bean
    public RestHighLevelClient client() {
        RestClient.builder(HttpHost.create("http://localhost:9200"));
        return new RestHighLevelClient(RestClient.builder(HttpHost.create("http://localhost:9200")));
    }
}