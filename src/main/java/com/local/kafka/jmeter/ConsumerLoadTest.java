package com.local.kafka.jmeter;

import org.apache.jmeter.protocol.java.sampler.AbstractJavaSamplerClient;
import org.apache.jmeter.protocol.java.sampler.JavaSamplerContext;
import org.apache.jmeter.samplers.SampleResult;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.LongSerializer;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;
import java.util.stream.IntStream;

public class ConsumerLoadTest extends AbstractJavaSamplerClient {

    private final static String TOPIC = "stringTopic";

    private static Producer<Long, String> createProducer() {
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(ProducerConfig.CLIENT_ID_CONFIG, "StringProducer");
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, LongSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        return new KafkaProducer<>(props);
    }

    public void produce() {

        Producer<Long, String> producer = createProducer();

        IntStream.range(1, 100).forEach(index -> {

            System.out.println("Producing message "+index);

            ProducerRecord<Long, String> record = new ProducerRecord<Long, String>(TOPIC,
                "This is record " + index);
            producer.send(record);
        });

        producer.flush();
        producer.close();
    }

    public static void main(String [] args){
        ConsumerLoadTest producerLoadTest = new ConsumerLoadTest();
        producerLoadTest.produce();
    }

    @Override
    public SampleResult runTest(JavaSamplerContext context) {

        produce();

        return null;
    }
}
