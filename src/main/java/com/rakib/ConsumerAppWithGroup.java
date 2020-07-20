package com.rakib;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;

public class ConsumerAppWithGroup {


    public static void main(String[] args) {
        Logger log = LoggerFactory.getLogger(ConsumerAppWithGroup.class);
        //consumer Config
        Properties properties = new Properties();
        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, Constant.SERVER_LIST);
        properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, Constant.CONSUMER_GROUP_ID_ONE);
        properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, Constant.CONSUMER_AUTO_OFFSET_RESET);

        //create Consumers
        KafkaConsumer<String, String> consumer = new KafkaConsumer<String, String>(properties);

        //subsribe consumer to topic
        consumer.subscribe(Arrays.asList(Constant.TOPIC_INFO, Constant.TOPIC_USER));

        //pull for new data
        while (true) {
            ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));
            for (ConsumerRecord<String, String> record : records) {
                log.info("Key:---- "+ record.key() + "  Value:---- "+ record.value());
            }
        }


    }
}
