package com.atguigu.spbt.demo.mapper;

import com.atguigu.spbt.demo.bean.Customer;
import com.baomidou.dynamic.datasource.annotation.DS;
import com.baomidou.mybatisplus.core.mapper.BaseMapper;
import org.apache.ibatis.annotations.*;

import java.util.List;

@Mapper
@DS("mysql0224")
public interface CustomerMapper extends BaseMapper<Customer> {

    @Insert("insert into customer(name,age) values (#{customer.name}, #{customer.age} )")
    public void insertCustomer(@Param("customer") Customer customer);


    @Insert("insert into customer(name,age) values (#{customer.name}, #{customer.age} )")
    @DS("mysql0111")
    public void insertCustomer0111(@Param("customer") Customer customer);


    @Select("select * from customer")
    public List<Customer> selectCustomerList();
}
