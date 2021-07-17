package com.atguigu.spbt.demo.service;

import com.atguigu.spbt.demo.bean.Customer;
import com.baomidou.mybatisplus.extension.service.IService;

public interface CustomerService extends IService<Customer> {

    public void saveCustomer(Customer customer);

    public  Customer getCustomerById( String customerId);
}
