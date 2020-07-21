package com.rakib;

import com.google.gson.JsonParser;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.KStream;

import java.util.Properties;

public class KafkaStreamApp {
    private static JsonParser jsonParser = new JsonParser();

    public static void main(String[] args) {

        //create properties
        Properties properties = new Properties();
        properties.setProperty(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, Constant.SERVER_LIST);
        properties.setProperty(StreamsConfig.APPLICATION_ID_CONFIG, "Kafka_Stream_Demo");
        properties.setProperty(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.ShortSerde.class.getName());
        properties.setProperty(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.ShortSerde.class.getName());

        //create topology
        StreamsBuilder builder = new StreamsBuilder();

        //input topic
        KStream<String, String> inputStream = builder.stream(Constant.TOPIC_INFO);
        KStream<String, String> filteredStream = inputStream.filter((key, value) -> Boolean.parseBoolean(jsonParse(value)));
        filteredStream.to(Constant.TOPIC_USER);

        //build topology
        KafkaStreams streams = new KafkaStreams(builder.build(), properties);

        //start stream application
        streams.start();

    }

    public static String jsonParse(String message) {

        try {
            return jsonParser.parse(message)
                    .getAsJsonObject()
                    .getAsString();
        } catch (NullPointerException e) {
            return e.getMessage();
        }

    }
}
