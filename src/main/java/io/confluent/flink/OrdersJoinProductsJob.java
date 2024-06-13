/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.confluent.flink;

import functons.EnrichmentJoinUsingMapState;
import models.Orders;
import models.OrdersWithProducts;
import models.Products;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.connector.base.DeliveryGuarantee;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.formats.json.JsonDeserializationSchema;
import org.apache.flink.formats.json.JsonSerializationSchema;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.kafka.clients.consumer.OffsetResetStrategy;

import java.io.InputStream;
import java.util.Properties;

public class OrdersJoinProductsJob {

	public static void main(String[] args) throws Exception {

		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

		Properties consumerConfig = new Properties();
		try (InputStream stream = OrdersJoinProductsJob.class.getClassLoader().getResourceAsStream("consumer.properties")) {
			consumerConfig.load(stream);
		}

		KafkaSource<Orders> ordersSource = KafkaSource.<Orders>builder()
				.setProperties(consumerConfig)
				.setTopics("orders")
				.setGroupId("group-orders")
				.setStartingOffsets(OffsetsInitializer.committedOffsets(OffsetResetStrategy.EARLIEST))
				.setValueOnlyDeserializer(new JsonDeserializationSchema<>(Orders.class))
				.build();

		KafkaSource<Products> productsSource = KafkaSource.<Products>builder()
				.setProperties(consumerConfig)
				.setTopics("products")
				.setGroupId("group-products")
				.setStartingOffsets(OffsetsInitializer.earliest())
				.setValueOnlyDeserializer(new JsonDeserializationSchema<>(Products.class))
				.build();

		DataStream<Orders> ordersStream = env
				.fromSource(ordersSource, WatermarkStrategy.noWatermarks(), "orders-source");

		DataStream<Products> productsStream = env
				.fromSource(productsSource, WatermarkStrategy.noWatermarks(), "products-source");

		Properties producerConfig = new Properties();
		try (InputStream stream = OrdersJoinProductsJob.class.getClassLoader().getResourceAsStream("producer.properties")) {
			producerConfig.load(stream);
		}

		KafkaRecordSerializationSchema<OrdersWithProducts> ordersWithProductsSerializer = KafkaRecordSerializationSchema.<OrdersWithProducts>builder()
				.setTopic("orders-with-products")
				.setValueSerializationSchema(new JsonSerializationSchema<>(
						() -> new ObjectMapper().registerModule(new JavaTimeModule())
				))
				.build();

		KafkaSink<OrdersWithProducts> ordersWithProductsSink = KafkaSink.<OrdersWithProducts>builder()
				.setKafkaProducerConfig(producerConfig)
				.setRecordSerializer(ordersWithProductsSerializer)
				.setDeliveryGuarantee(DeliveryGuarantee.EXACTLY_ONCE)
				.setTransactionalIdPrefix("OrdersJoinProducts")
				.build();

		ordersStream
				.connect(productsStream)
				.keyBy(o -> o.productId, p -> p.productId)
				//.process(new EnrichmentJoinUsingListState());
				.process(new EnrichmentJoinUsingMapState())
				.sinkTo(ordersWithProductsSink);

		env.execute("OrdersJoinProductsJob");
	}

}