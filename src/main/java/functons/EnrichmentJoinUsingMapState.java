package functons;

import models.Orders;
import models.OrdersWithProducts;
import models.Products;
import org.apache.flink.api.common.state.*;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.co.KeyedCoProcessFunction;
import org.apache.flink.util.Collector;

import java.util.UUID;

public class EnrichmentJoinUsingMapState
        extends KeyedCoProcessFunction<String, Orders, Products, OrdersWithProducts> {

    private ValueState<Products> productsState;
    private MapState<String, Orders> ordersState;

    @Override
    public void open(Configuration parameters) {

        ordersState = getRuntimeContext().getMapState(new MapStateDescriptor<>("orders state", String.class, Orders.class));
        productsState = getRuntimeContext().getState(new ValueStateDescriptor<>("products state", Products.class));
    }

    @Override
    public void processElement1(Orders orders, Context context, Collector<OrdersWithProducts> out) throws Exception {

        Products savedProducts = productsState.value();
        if (savedProducts == null) {
            ordersState.put(UUID.randomUUID().toString(), orders);
        } else {
            out.collect(new OrdersWithProducts(orders, savedProducts));
        }
    }

    @Override
    public void processElement2(Products products, Context context, Collector<OrdersWithProducts> out) throws Exception {

        Products savedProducts = productsState.value();
        if (savedProducts == null) {
            ordersState.iterator()
                    .forEachRemaining(o -> out.collect(new OrdersWithProducts(o.getValue(), products)));
            ordersState.clear();
            productsState.update(products);
        } else {
            if (products.productTs > savedProducts.productTs) {
                productsState.update(products);
            }
        }
    }

}