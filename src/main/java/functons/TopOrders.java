package functons;

import models.OrdersWindowStatistics;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

import java.util.ArrayList;
import java.util.List;
import java.sql.Timestamp;

public class TopOrders
        extends KeyedProcessFunction<Long, OrdersWindowStatistics, String> {

    private ListState<OrdersWindowStatistics> topProductsState;

    @Override
    public void open(Configuration parameters) {

        topProductsState = getRuntimeContext().getListState(new ListStateDescriptor<>("top products state", OrdersWindowStatistics.class));
    }

    @Override
    public void processElement(OrdersWindowStatistics input, Context context, Collector<String> collector) throws Exception {

        topProductsState.add(input);
        context.timerService().registerEventTimeTimer(input.windowEnd + 1);
    }

    @Override
    public void onTimer(long timestamp, OnTimerContext ctx, Collector<String> out) throws Exception {

        List<OrdersWindowStatistics> allProducts = new ArrayList<>();
        for (OrdersWindowStatistics ows : topProductsState.get()) {
            allProducts.add(ows);
        }

        topProductsState.clear();

        allProducts.sort((o1, o2) -> (int) (o2.orderCount - o1.orderCount));

        StringBuilder result = new StringBuilder();
        result.append("WindowEnd: ").append(new Timestamp(timestamp - 1)).append("\n");
        for (int i = 0; i < 3; i++) {
            OrdersWindowStatistics ows = allProducts.get(i);
            result.append("productId: ").append(ows.productId)
                    .append(" orderCount: ").append(ows.orderCount)
                    .append(" orderValue: ").append(ows.orderValue)
                    .append("\n");
        }

        out.collect(result.toString());
    }

}