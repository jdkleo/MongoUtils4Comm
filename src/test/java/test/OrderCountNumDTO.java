package test;

import java.io.Serializable;

/**
 * Created by ody on 2016/10/24.
 */
public class OrderCountNumDTO implements Serializable {

    private String orderDate;//日期

    private Integer countNum;//订单总数

    public String getOrderDate() {
        return orderDate;
    }

    public void setOrderDate(String orderDate) {
        this.orderDate = orderDate;
    }

    public Integer getCountNum() {
        return countNum;
    }

    public void setCountNum(Integer countNum) {
        this.countNum = countNum;
    }
}
