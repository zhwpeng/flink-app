package models;

import java.math.BigDecimal;

public class OrdersStatistics {
    public String productId;
    public String productName;
    public Long orderCount;
    public BigDecimal orderValue;

    public OrdersStatistics() {}

    public OrdersStatistics(OrdersWithProducts ordersWithProducts) {
        this.productId = ordersWithProducts.productId;
        this.productName = ordersWithProducts.productName;
        this.orderCount = 1L;
        this.orderValue = ordersWithProducts.orderValue;
    }

    @Override
    public String toString() {
        return "OrdersStatistics{" +
                "productId='" + productId + '\'' +
                ", productName='" + productName + '\'' +
                ", orderCount='" + orderCount + '\'' +
                ", orderValue='" + orderValue + '\'' +
                '}';
    }

    public OrdersStatistics merge(OrdersStatistics that) {

        OrdersStatistics merged = new OrdersStatistics();

        merged.productId = this.productId;
        merged.productName = this.productName;
        merged.orderCount = this.orderCount + that.orderCount;
        merged.orderValue = this.orderValue.add(that.orderValue);

        return merged;
    }

}