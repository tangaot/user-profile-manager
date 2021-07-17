package com.atguigu.spbt.demo.service.impl;

import com.atguigu.spbt.demo.bean.Customer;
import com.atguigu.spbt.demo.mapper.CustomerMapper;
import com.atguigu.spbt.demo.service.CustomerService;
import com.baomidou.mybatisplus.core.conditions.query.QueryWrapper;
import com.baomidou.mybatisplus.extension.service.impl.ServiceImpl;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.List;

@Service
public class CustomerServiceImpl extends ServiceImpl<CustomerMapper,Customer> implements CustomerService  {

    @Autowired
    CustomerMapper customerMapper;

    public  Customer getCustomerById( String customerId){
        Customer customer = customerMapper.selectById(customerId);
        return  customer;
    }

    @Override
    public void saveCustomer(Customer customer) {
        System.out.println("service :saveCustomer:" + customer);
      //  customerMapper.insertCustomer(customer);
        customerMapper.insert(customer);

        //向另一个数据源插入数据
        customerMapper.insertCustomer0111(customer);

      //  List<Customer> customerList = customerMapper.selectCustomerList();
        List<Customer> customerList1 = customerMapper.selectList(new QueryWrapper<Customer>().eq("name","zhao6"));


        System.out.println(customerList1);


    }


}
