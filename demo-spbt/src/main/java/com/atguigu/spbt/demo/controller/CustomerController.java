package com.atguigu.spbt.demo.controller;


import com.atguigu.spbt.demo.bean.Customer;
import com.atguigu.spbt.demo.service.CustomerService;
import com.atguigu.spbt.demo.service.impl.CustomerServiceImpl;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.*;

@RestController //@Controller
public class CustomerController {

    @Autowired
    CustomerService customerService ;

    //参数 三种  1 、 路径 ?xxx=xxx&y=xxx&fa=xx   2、 /customer/123   3、 请求体中的参数 一般是json
    @RequestMapping("/hello")
    public String helloCustomer(@RequestParam("name") String name){
        return "hello world:"+name;
    }


    @GetMapping("/customer/{id}")
    public String getCustomer(@PathVariable("id") String customerId){
       // Customer customer = customerService.getCustomerById(customerId);
        Customer customerByMP = customerService.getById(customerId);

        return "hello world: customer:"+customerByMP;
    }

    @PostMapping("/customer")
    public String saveCustomer( @RequestBody Customer customer){
        // 会根据主键是否存在 来决定 插入还是修改
       // customerService.saveOrUpdate(customer);  //直接考mybatis-plus 生成service层的方法  //标准的插删改查 service层都不用写
         customerService.saveCustomer(customer);
        return "save: customer:"+customer.toString();
    }

}
