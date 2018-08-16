package com.cattle34.study.day03.topn;



import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class OrderBeen implements Writable {
    //
    String ordernum;
     String UID;
     String product;
     String sku;
     double price;

    public OrderBeen() {
    }

    public OrderBeen(String ordernum, String UID, String product, String sku, double price) {
        this.ordernum = ordernum;
        this.UID = UID;
        this.product = product;
        this.sku = sku;
        this.price = price;
    }

    @Override
    public String toString() {
        return "OrderBeen{" +
                "ordernum='" + ordernum + '\'' +
                ", UID='" + UID + '\'' +
                ", product='" + product + '\'' +
                ", sku='" + sku + '\'' +
                ", price=" + price +
                '}';
    }

    public String getOrdernum() {
        return ordernum;
    }

    public void setOrdernum(String ordernum) {
        this.ordernum = ordernum;
    }

    public String getUID() {
        return UID;
    }

    public void setUID(String UID) {
        this.UID = UID;
    }

    public String getProduct() {
        return product;
    }

    public void setProduct(String product) {
        this.product = product;
    }

    public String getSku() {
        return sku;
    }

    public void setSku(String sku) {
        this.sku = sku;
    }

    public double getPrice() {
        return price;
    }

    public void setPrice(double price) {
        this.price = price;
    }



    @Override
    public void write(DataOutput out) throws IOException {
    out.writeUTF(this.ordernum);
    out.writeUTF(this.product);
    out.writeUTF(this.sku);
    out.writeUTF(this.UID);
    out.writeDouble(this.price);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        this.ordernum=in.readUTF();
        this.product=in.readUTF();
        this.sku=in.readUTF();
        this.UID=in.readUTF();
        this.price=in.readDouble();
    }
}
