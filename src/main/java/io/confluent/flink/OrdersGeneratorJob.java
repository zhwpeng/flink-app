package io.confluent.flink;

import models.Orders;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.connector.source.util.ratelimit.RateLimiterStrategy;
import org.apache.flink.connector.base.DeliveryGuarantee;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.formats.json.JsonSerializationSchema;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.connector.datagen.source.DataGeneratorSource;

import java.io.InputStream;
import java.util.Properties;

public class OrdersGeneratorJob {

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        Properties producerConfig = new Properties();
        try (InputStream stream = OrdersGeneratorJob.class.getClassLoader().getResourceAsStream("producer.properties")) {
            producerConfig.load(stream);
        }

        DataGeneratorSource<Orders> ordersSource =
                new DataGeneratorSource<>(
                        index -> OrdersGenerator.generateOrders(),
                        Long.MAX_VALUE,
                        RateLimiterStrategy.perSecond(1),
                        Types.POJO(Orders.class)
                );

        DataStream<Orders> ordersStream = env.
                fromSource(ordersSource, WatermarkStrategy.forMonotonousTimestamps(), "orders-source");

        KafkaRecordSerializationSchema<Orders> ordersSerializer = KafkaRecordSerializationSchema.<Orders>builder()
                .setTopic("orders")
                .setValueSerializationSchema(new JsonSerializationSchema<>(
                        () -> new ObjectMapper().registerModule(new JavaTimeModule())
                ))
                .build();

        KafkaSink<Orders> ordersSink = KafkaSink.<Orders>builder()
                .setKafkaProducerConfig(producerConfig)
                .setRecordSerializer(ordersSerializer)
                .setDeliveryGuarantee(DeliveryGuarantee.AT_LEAST_ONCE)
                .build();

        ordersStream
                .sinkTo(ordersSink)
                .name("orders-sink");

        env.execute("OrdersGeneratorJob");
    }

}