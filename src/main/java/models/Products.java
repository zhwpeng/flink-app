package models;

public class Products {
    public String productId;
    public String productName;
    public Long productTs;

    public Products() {}

    public Products(String productId, String productName, Long productTs) {
        this.productId = productId;
        this.productName = productName;
        this.productTs = productTs;
    }

    @Override
    public String toString() {
        return "Products{" +
                "productId='" + productId + '\'' +
                ", productName='" + productName + '\'' +
                ", productTs='" + productTs + '\'' +
                '}';
    }

}