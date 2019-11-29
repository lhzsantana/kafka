package com.local.kafka.avro;

import com.local.kafka.Message;
import io.confluent.kafka.serializers.KafkaAvroSerializer;
import io.confluent.kafka.serializers.KafkaAvroSerializerConfig;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.LongSerializer;

import java.util.Properties;
import java.util.stream.IntStream;

public class AvroProducer    {

    private final static String TOPIC = "avro2";

    private static Producer<Long, Message> createProducer() {
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(ProducerConfig.CLIENT_ID_CONFIG, "AvroProducer");
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, LongSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, KafkaAvroSerializer.class.getName());
        //props.put(KafkaAvroSerializerConfig.SCHEMA_REGISTRY_URL_CONFIG, "http://localhost:8081");

        //props.put(AbstractKafkaAvroSerDeConfig.AUTO_REGISTER_SCHEMAS, false);

        return new KafkaProducer<>(props);
    }

    public static void main(String... args) {

        Producer<Long, Message> producer = createProducer();

        Message message = Message.newBuilder()
                .setText("This is a text.")
                .build();

        IntStream.range(1, 100).forEach(index -> {
            producer.send(new ProducerRecord<>(TOPIC, 1L * index, message));
        });

        producer.flush();
        producer.close();
    }
}