# Flink DataStream API Exmaples

0. Schema
```
   Orders {
     public String orderId;
     public String productId;
     public BigDecimal orderValue;
     public Long orderTs;
   }
   
   Products {
     public String productId;
     public String productName;
     public Long productTs;
   }
```

1. OrdersGeneratorJob.java: Use DataGenerator connector to generate mock data.
2. OrdersJoinProductsJob.java: Join orders with products. If corresponding products are not available, orders will be buffered.
3. OrdersAggregateJob.java: Calculate total order values by incremental aggregation (ReduceFunction follows by ProcessWindowFunction). We also use side-output to capture late orders.
4. OrdersTopJob.java: List top 3 popular products by Sliding Window, AggregateFunction and WindowFunction.