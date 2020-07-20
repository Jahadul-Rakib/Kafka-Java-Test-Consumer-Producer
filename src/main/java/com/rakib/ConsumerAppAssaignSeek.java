package com.rakib;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Arrays;
import java.util.Collections;
import java.util.Properties;

public class ConsumerAppAssaignSeek {


    public static void main(String[] args) {
        Logger log = LoggerFactory.getLogger(ConsumerAppAssaignSeek.class);
        //consumer Config
        Properties properties = new Properties();
        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, Constant.SERVER_LIST);
        properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, Constant.CONSUMER_AUTO_OFFSET_RESET);

        //create Consumers
        KafkaConsumer<String, String> consumer = new KafkaConsumer<String, String>(properties);

        TopicPartition readFromPartitionOne = new TopicPartition(Constant.TOPIC_INFO, 0);
        TopicPartition readFromPartitionTwo = new TopicPartition(Constant.TOPIC_INFO, 1);
        TopicPartition readFromPartitionThree = new TopicPartition(Constant.TOPIC_INFO, 2);

        //assign
        consumer.assign(Arrays.asList(readFromPartitionOne, readFromPartitionTwo, readFromPartitionThree));
        //seek
        consumer.seekToEnd(Arrays.asList(readFromPartitionOne, readFromPartitionTwo, readFromPartitionThree));

        //pull for new data
        while (true) {
            ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));
            for (ConsumerRecord<String, String> record : records) {
                log.info("Key:---- " + record.key() + "  Value:---- " + record.value());
            }
        }


    }
}
