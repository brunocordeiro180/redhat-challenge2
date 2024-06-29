package br.com.redhat.streamer.component;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.*;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.xcontent.XContentType;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.io.IOException;
import java.util.Arrays;

@Component
public class MessagesProcessor {

    private static final Serde<String> STRING_SERDE = Serdes.String();

    @Autowired
    private RestHighLevelClient client;

    @Autowired
    void buildPipeline(StreamsBuilder streamsBuilder) {

        ObjectMapper objectMapper = new ObjectMapper();

        KStream<String, String> messageStream = streamsBuilder
          .stream("movies", Consumed.with(STRING_SERDE, STRING_SERDE));

        KStream<String, String> transformedStream = messageStream.mapValues(value -> {
            try {
                JsonNode jsonNode = objectMapper.readTree(value);
                ((ObjectNode) jsonNode).remove(Arrays.asList("href", "extract", "thumbnail", "thumbnail_width", "thumbnail_height"));
                return objectMapper.writeValueAsString(jsonNode);
            } catch (IOException e) {
                e.printStackTrace();
                return value;
            }
        });

        transformedStream.foreach((key, value) -> {
            IndexRequest request = new IndexRequest("movies-index")
                    .source(value, XContentType.JSON);
            try {
                client.index(request, RequestOptions.DEFAULT);
            } catch (IOException e) {
                e.printStackTrace();
            }
        });
    }
}