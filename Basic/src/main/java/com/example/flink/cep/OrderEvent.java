package com.example.flink.cep;


public class OrderEvent {
    public String userId; //用户ID
    public String orderId; //订单ID
    public String eventType; //事件类型（操作类型）
    public Long timestamp; //时间戳

    public OrderEvent() {
    }

    public OrderEvent(String userId, String orderId, String eventType, Long timestamp) {
        this.userId = userId;
        this.orderId = orderId;
        this.eventType = eventType;
        this.timestamp = timestamp;
    }

    @Override
    public String toString() {
        return "OrderEvent[" +
                "userId='" + userId + '\'' +
                "orderId='" + orderId + '\'' +
                ", eventType='" + eventType + '\'' +
                ", timestamp=" + timestamp +
                ']';
    }
}

