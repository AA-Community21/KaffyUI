package com.kafkatest.kaffyapp;

import jakarta.annotation.PreDestroy;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.util.Properties;

@Configuration
public class KaffyConfig {

    @Value("${kafka.bootstrap.servers}")
    private String bootstrapServers;

    private KafkaProducer<String, String> producer;

    private KafkaConsumer<String, String> consumer;

    private static final String SASL_JAAS_CONFIG = "sasl.jaas.config";

    private static final String SASL_MECHANISM = "sasl.mechanism";

    private static final String SECURITY_PROTOCOL = AdminClientConfig.SECURITY_PROTOCOL_CONFIG;

    private static final String PLAIN = "PLAIN";

    private static final String SASL_SSL = "SASL_SSL";




    @Bean
    public AdminClient getAdminClient() {
        Properties props = new Properties();
        props.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(SECURITY_PROTOCOL, SASL_SSL);
        props.put(SASL_JAAS_CONFIG, "org.apache.kafka.common.security.plain.PlainLoginModule required username='PKFS7CSZAK6526IA' password='cflt2wWAcEKfsRyaw4xiCGG7odo8yUZ6t5t2l/2HpZk12s4vwJWpvvTfoVR/5N/g';");
        props.put(SASL_MECHANISM, PLAIN);
        props.put(AdminClientConfig.REQUEST_TIMEOUT_MS_CONFIG, "5000");
        props.put(AdminClientConfig.RETRIES_CONFIG, "3");
        return AdminClient.create(props);
    }

    @Bean
    public KafkaConsumer<String, String> consumerProperties() {
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(SECURITY_PROTOCOL, SASL_SSL);
        props.put(SASL_JAAS_CONFIG, "org.apache.kafka.common.security.plain.PlainLoginModule required username='PKFS7CSZAK6526IA' password='cflt2wWAcEKfsRyaw4xiCGG7odo8yUZ6t5t2l/2HpZk12s4vwJWpvvTfoVR/5N/g';");
        props.put(SASL_MECHANISM, PLAIN);
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "kafka-dashboard-consumer");
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);
        props.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, 100);
        props.put(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG, 30000);
        this.consumer = new KafkaConsumer<>(props);
        return consumer;
    }

    @Bean
    public KafkaProducer<String, String> kafkaProducer() {
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(SECURITY_PROTOCOL, SASL_SSL);
        props.put(SASL_JAAS_CONFIG, "org.apache.kafka.common.security.plain.PlainLoginModule required username='PKFS7CSZAK6526IA' password='cflt2wWAcEKfsRyaw4xiCGG7odo8yUZ6t5t2l/2HpZk12s4vwJWpvvTfoVR/5N/g';");
        props.put(SASL_MECHANISM, PLAIN);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.ACKS_CONFIG, "all");
        props.put(ProducerConfig.RETRIES_CONFIG, 3);
        props.put(ProducerConfig.BATCH_SIZE_CONFIG, 16384);
        props.put(ProducerConfig.LINGER_MS_CONFIG, 1);
        props.put(ProducerConfig.BUFFER_MEMORY_CONFIG, 33554432);
        this.producer = new KafkaProducer<>(props);
        return this.producer;
    }

    @PreDestroy
    public void cleanup() {
        if (producer != null) {
            producer.close();
        }
    }
}
