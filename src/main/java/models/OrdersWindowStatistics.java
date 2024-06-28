package models;

import java.math.BigDecimal;

public class OrdersWindowStatistics {
    public String productId;
    public Long windowEnd;
    public Long orderCount;
    public BigDecimal orderValue;

    public OrdersWindowStatistics() {}

    public OrdersWindowStatistics(String productId, Long windowEnd, Long orderCount, BigDecimal orderValue) {
        this.productId = productId;
        this.windowEnd = windowEnd;
        this.orderCount = orderCount;
        this.orderValue = orderValue;
    }

    @Override
    public String toString() {
        return "OrdersWindowStatistics{" +
                "productId='" + productId + '\'' +
                ", windowEnd='" + windowEnd + '\'' +
                ", orderCount='" + orderCount + '\'' +
                ", orderValue='" + orderValue + '\'' +
                '}';
    }

}