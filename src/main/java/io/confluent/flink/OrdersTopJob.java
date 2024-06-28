package io.confluent.flink;

import functons.OrdersWindowAggregate;
import functons.OrdersWindowResult;
import functons.TopOrders;
import models.OrdersWithProducts;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.formats.json.JsonDeserializationSchema;
import org.apache.flink.streaming.api.datastream.DataStream;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.SlidingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.kafka.clients.consumer.OffsetResetStrategy;

import java.io.InputStream;
import java.util.Properties;

public class OrdersTopJob {

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        Properties consumerConfig = new Properties();
        try (InputStream stream = OrdersTopJob.class.getClassLoader().getResourceAsStream("consumer.properties")) {
            consumerConfig.load(stream);
        }

        KafkaSource<OrdersWithProducts> ordersWithProductsSource = KafkaSource.<OrdersWithProducts>builder()
                .setProperties(consumerConfig)
                .setTopics("orders-with-products")
                .setGroupId("group-orders-top")
                .setStartingOffsets(OffsetsInitializer.committedOffsets(OffsetResetStrategy.EARLIEST))
                .setValueOnlyDeserializer(new JsonDeserializationSchema<>(OrdersWithProducts.class))
                .build();

        DataStream<OrdersWithProducts> ordersWithProductsStream = env
                .fromSource(ordersWithProductsSource, WatermarkStrategy.<OrdersWithProducts>forMonotonousTimestamps().withTimestampAssigner(
                        (element, recordTimestamp) -> element.orderTs), "orders-with-products-source");

        defineWorkflow(ordersWithProductsStream)
                .print()
                .name("orders-top-sink");

        env.execute("OrdersTopJob");
    }

    public static DataStream<String> defineWorkflow(DataStream<OrdersWithProducts> ordersWithProducts) {
        return ordersWithProducts
                .keyBy(owp -> owp.productId)
                .window(SlidingEventTimeWindows.of(Time.minutes(1), Time.seconds(30)))
                .aggregate(new OrdersWindowAggregate(), new OrdersWindowResult())
                .keyBy(ows -> ows.windowEnd)
                .process(new TopOrders());
    }

}