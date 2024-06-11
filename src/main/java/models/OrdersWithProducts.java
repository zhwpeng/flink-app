package models;

import java.math.BigDecimal;

public class OrdersWithProducts {
    public String orderId;
    public String productId;
    public String productName;
    public BigDecimal orderValue;
    public Long orderTs;
    public Long productTs;

    public OrdersWithProducts() {}

    public OrdersWithProducts(Orders orders, Products products) {
        this.orderId = orders.orderId;
        this.productId = orders.productId;
        this.productName = products.productName;
        this.orderValue = orders.orderValue;
        this.orderTs = orders.orderTs;
        this.productTs = products.productTs;
    }

    @Override
    public String toString() {
        return "OrdersWithProducts{" +
                "orderId='" + orderId + '\'' +
                ", productId='" + productId + '\'' +
                ", productName='" + productName + '\'' +
                ", orderValue='" + orderValue + '\'' +
                ", orderTs='" + orderTs + '\'' +
                ", productTs='" + productTs + '\'' +
                '}';
    }

}