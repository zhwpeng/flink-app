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
2. OrdersJoinProductsJob.java: Join DataStream orders with products. If corresponding products are not available, orders will be buffered. We also demonstrate how to use side-output in this example.
3. OrdersAggregateJob.java: Calculate total order values by incremental aggregation (ReduceFunction follows by ProcessWindowFunction).