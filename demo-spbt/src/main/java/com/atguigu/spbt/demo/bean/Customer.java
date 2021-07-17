package com.atguigu.spbt.demo.bean;

import com.baomidou.mybatisplus.annotation.IdType;
import com.baomidou.mybatisplus.annotation.TableId;
import lombok.Data;

@Data
public class Customer {

    @TableId(value = "id" ,type = IdType.AUTO)
    String id;

    String name;

    int age;

}
