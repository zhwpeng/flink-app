package io.confluent.flink;

import models.Orders;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.util.Random;

public class OrdersGenerator {
    private static final Random random = new Random();

    private static int generateRandomNumber(int min, int max) {
        return random.nextInt(max - min) + min;
    }

    private static String generateOrderId() {
        return String.valueOf(generateRandomNumber(1000, 2000));
    }

    private static String generateProductId() {
        return String.valueOf(generateRandomNumber(100, 110));
    }

    private static BigDecimal generateOrderValue() {
        return BigDecimal.valueOf(random.nextDouble() * 100).setScale(2, RoundingMode.HALF_EVEN);
    }

    private static Long generateOrderTs() {
        return System.currentTimeMillis();
    }

    public static Orders generateOrders() {
        return new Orders(generateOrderId(), generateProductId(), generateOrderValue(), generateOrderTs());
    }

}