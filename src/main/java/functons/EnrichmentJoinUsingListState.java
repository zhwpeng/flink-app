package functons;

import models.Orders;
import models.OrdersWithProducts;
import models.Products;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.co.KeyedCoProcessFunction;
import org.apache.flink.util.Collector;

public class EnrichmentJoinUsingListState
        extends KeyedCoProcessFunction<String, Orders, Products, OrdersWithProducts> {

        private ValueState<Products> productsState;
        private ListState<Orders> ordersState;

        @Override
        public void open(Configuration parameters) {

            ordersState = getRuntimeContext().getListState(new ListStateDescriptor<>("orders state", Orders.class));
            productsState = getRuntimeContext().getState(new ValueStateDescriptor<>("products state", Products.class));
        }

        @Override
        public void processElement1(Orders orders, Context context, Collector<OrdersWithProducts> out) throws Exception {

            Products savedProducts = productsState.value();
            if (savedProducts == null) {
                ordersState.add(orders);
            } else {
                out.collect(new OrdersWithProducts(orders, savedProducts));
            }
        }

        @Override
        public void processElement2(Products products, Context context, Collector<OrdersWithProducts> out) throws Exception {

            Products savedProducts = productsState.value();
            if (savedProducts == null) {
                ordersState.get()
                        .forEach(o -> out.collect(new OrdersWithProducts(o, products)));
                ordersState.clear();
                productsState.update(products);
            } else {
                if (products.productTs > savedProducts.productTs) {
                    productsState.update(products);
                }
            }
        }

}