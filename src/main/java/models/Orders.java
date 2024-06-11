package models;

import java.math.BigDecimal;

public class Orders {
    public String orderId;
    public String productId;
    public BigDecimal orderValue;
    public Long orderTs;

    public Orders() {}

    public Orders(String orderId, String productId, BigDecimal orderValue, Long orderTs) {
        this.orderId = orderId;
        this.productId = productId;
        this.orderValue = orderValue;
        this.orderTs = orderTs;
    }

    @Override
    public String toString() {
        return "Orders{" +
                "orderId='" + orderId + '\'' +
                ", productId='" + productId + '\'' +
                ", orderValue='" + orderValue + '\'' +
                ", orderTs='" + orderTs + '\'' +
                '}';
    }

}