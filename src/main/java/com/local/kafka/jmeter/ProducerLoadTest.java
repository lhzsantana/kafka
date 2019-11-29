package com.local.kafka.jmeter;

import com.local.kafka.Message;
import io.confluent.kafka.serializers.KafkaAvroDeserializer;
import io.confluent.kafka.serializers.KafkaAvroDeserializerConfig;
import io.confluent.kafka.serializers.KafkaAvroSerializer;
import org.apache.jmeter.protocol.java.sampler.AbstractJavaSamplerClient;
import org.apache.jmeter.protocol.java.sampler.JavaSamplerContext;
import org.apache.jmeter.samplers.SampleResult;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.LongDeserializer;
import org.apache.kafka.common.serialization.LongSerializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Collections;
import java.util.Properties;
import java.util.stream.IntStream;

public class ProducerLoadTest extends AbstractJavaSamplerClient {

    private final static String TOPIC = "stringTopic";

    private static Consumer<Long, String> createConsumer() {
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "StringConsumer");
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, LongDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(KafkaAvroDeserializerConfig.SPECIFIC_AVRO_READER_CONFIG, "true");

        return new KafkaConsumer<>(props);
    }

    public void consume() {

        final Consumer<Long, String> consumer = createConsumer();
        consumer.subscribe(Collections.singletonList(TOPIC));

        IntStream.range(1, 100).forEach(index -> {

            final ConsumerRecords<Long, String> records =
                consumer.poll(100);

            consumer.subscribe(Collections.singletonList(TOPIC));

            if (records.count() == 0) {
                System.out.println("None found");
            } else records.forEach(record -> {

                String messageRecord = record.value();

                System.out.printf("%s %d %d %s \n", record.topic(), record.partition(), record.offset(), messageRecord);
            });
        });
    }

    public static void main(String [] args){
        ProducerLoadTest producerLoadTest = new ProducerLoadTest();
        producerLoadTest.consume();
    }

    @Override
    public SampleResult runTest(JavaSamplerContext context) {

        consume();

        return null;
    }
}
