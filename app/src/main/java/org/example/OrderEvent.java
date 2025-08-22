package org.example;

public class OrderEvent {
    public String order_id;
    public String symbol;
    public String side;
    public long   qty;
    public long   price;
    public long   ts_ns_send;
    public String correlation_id;

    public OrderEvent() {}
}
