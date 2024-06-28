package functons;

import org.apache.flink.streaming.api.functions.ProcessFunction;
import models.OrdersWithProducts;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

public class LateOrdersSideOutput
        extends ProcessFunction<OrdersWithProducts, OrdersWithProducts> {

    private final OutputTag<OrdersWithProducts> lateOrdersTag;

    public LateOrdersSideOutput(OutputTag<OrdersWithProducts> lateOrdersTag) {
        this.lateOrdersTag = lateOrdersTag;
    }

    @Override
    public void processElement(OrdersWithProducts value, Context ctx, Collector<OrdersWithProducts> out){

        long currentWatermark=ctx.timerService().currentWatermark();
        long eventTimestamp=ctx.timestamp();

        if (eventTimestamp < currentWatermark) {
            ctx.output(lateOrdersTag, value);
        } else {
            out.collect(value);
        }
    }

}
