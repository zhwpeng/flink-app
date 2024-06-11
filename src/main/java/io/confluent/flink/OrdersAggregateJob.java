package io.confluent.flink;

import functons.IncrementalAggregate;
import models.OrdersStatistics;
import models.OrdersWithProducts;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.formats.json.JsonDeserializationSchema;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.kafka.clients.consumer.OffsetResetStrategy;

import java.io.InputStream;
import java.util.Properties;

public class OrdersAggregateJob {

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        Properties consumerConfig = new Properties();
        try (InputStream stream = OrdersAggregateJob.class.getClassLoader().getResourceAsStream("consumer.properties")) {
            consumerConfig.load(stream);
        }

        KafkaSource<OrdersWithProducts> ordersWithProductsSource = KafkaSource.<OrdersWithProducts>builder()
                .setProperties(consumerConfig)
                .setTopics("orders-with-products")
                .setGroupId("group-orders-with-products")
                .setStartingOffsets(OffsetsInitializer.committedOffsets(OffsetResetStrategy.EARLIEST))
                .setValueOnlyDeserializer(new JsonDeserializationSchema<>(OrdersWithProducts.class))
                .build();

        DataStream<OrdersWithProducts> ordersWithProductsDataStream = env
                .fromSource(ordersWithProductsSource, WatermarkStrategy.forMonotonousTimestamps(), "orders-with-products-source");

        ordersWithProductsDataStream
                .map(OrdersStatistics::new)
                .keyBy(os -> os.productId)
                .window(TumblingEventTimeWindows.of(Time.minutes(1)))
                .reduce(OrdersStatistics::merge, new IncrementalAggregate())
                .print()
                .name("orders-aggregate-sink");

        env.execute("OrdersAggregateJob");
    }

}