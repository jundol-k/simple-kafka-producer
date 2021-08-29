package com.example;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;
import java.util.concurrent.ExecutionException;

public class SimpleProducer {
    private final static Logger logger = LoggerFactory.getLogger(SimpleProducer.class);
    private final static String TOPIC_NAME = "test";
    private final static String BOOTSTRAP_SERVERS = "my-linux:9092";

    public static void main(String[] args) throws ExecutionException, InterruptedException {

        Properties configs = new Properties();
        configs.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS);
        configs.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        configs.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        configs.put(ProducerConfig.PARTITIONER_CLASS_CONFIG, CustomerPartitioner.class); // 커스텀 파티셔너를 지정하는 모습

        KafkaProducer<String, String> producer = new KafkaProducer<>(configs);

        //int partitionNo = 0; // 파티션 넘버 지정 가능

        String messageKey = "kang";
        String messageValue = "112";
        ProducerRecord<String, String> record = new ProducerRecord<>(TOPIC_NAME, messageKey, messageValue);
        // RecordMetadata metadata = producer.send(record).get(); // 동기적으로 send 메서드의 결과를 기다리는 코드
        //logger.info("metadata ==> {} ", metadata.toString());

        producer.send(record, new ProducerCallback()); // 비동기로 send 메서드의 결과를 콜백으로 받기
        producer.flush();
        logger.info("record ==> {}", record);

        producer.close();
    }
}
