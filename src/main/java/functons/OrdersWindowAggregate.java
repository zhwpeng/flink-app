package functons;

import models.OrdersWithProducts;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.java.tuple.Tuple2;

import java.math.BigDecimal;

public class OrdersWindowAggregate
        implements AggregateFunction<OrdersWithProducts, Tuple2<Long, BigDecimal>, Tuple2<Long, BigDecimal>> {

    @Override
    public Tuple2<Long, BigDecimal> createAccumulator() {
        return new Tuple2<>(0L, BigDecimal.valueOf(0.00));
    }

    @Override
    public Tuple2<Long, BigDecimal> merge(Tuple2<Long, BigDecimal> a, Tuple2<Long, BigDecimal> b) {

        a.f0 += b.f0;
        a.f1= a.f1.add(b.f1);
        return a;
    }

    @Override
    public Tuple2<Long, BigDecimal> add(OrdersWithProducts owp, Tuple2<Long, BigDecimal> acc) {

        acc.f0++;
        acc.f1= acc.f1.add(owp.orderValue);
        return acc;
    }

    @Override
    public Tuple2<Long, BigDecimal> getResult(Tuple2<Long, BigDecimal> acc) {
        return acc;
    }

}