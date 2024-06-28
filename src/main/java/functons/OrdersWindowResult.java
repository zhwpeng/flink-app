package functons;

import models.OrdersWindowStatistics;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.math.BigDecimal;

public class OrdersWindowResult
        implements WindowFunction<Tuple2<Long, BigDecimal>, OrdersWindowStatistics, String, TimeWindow> {

    @Override
    public void apply(String key, TimeWindow window, Iterable<Tuple2<Long, BigDecimal>> in, Collector<OrdersWindowStatistics> out) {

        Tuple2<Long, BigDecimal> ows = in.iterator().next();
        out.collect(new OrdersWindowStatistics(key, window.getEnd(), ows.f0, ows.f1));
    }

}
