package functons;


import models.OrdersStatistics;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

public class IncrementalAggregate
        extends ProcessWindowFunction<OrdersStatistics, OrdersStatistics, String, TimeWindow> {

    private ValueState<OrdersStatistics> ordersStatisticsState;

    @Override
    public void open(Configuration parameters) {

        ordersStatisticsState = getRuntimeContext().getState(new ValueStateDescriptor<>("order statistics state", OrdersStatistics.class));
    }

    @Override
    public void process(String productId, Context context, Iterable<OrdersStatistics> ordersStatisticsList, Collector<OrdersStatistics> collector) throws Exception {

        OrdersStatistics savedOrderStatistics = ordersStatisticsState.value();

        for (OrdersStatistics os: ordersStatisticsList) {
            if(savedOrderStatistics == null)
                savedOrderStatistics = os;
            else
                savedOrderStatistics = savedOrderStatistics.merge(os);
        }

        ordersStatisticsState.update(savedOrderStatistics);

        collector.collect(savedOrderStatistics);
    }

}