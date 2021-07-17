package com.atguigu.userprofile.service.impl;

import com.alibaba.fastjson.JSON;
import com.atguigu.userprofile.bean.TagCondition;
import com.atguigu.userprofile.bean.TagInfo;
import com.atguigu.userprofile.bean.UserGroup;
import com.atguigu.userprofile.constants.ConstCodes;
import com.atguigu.userprofile.mapper.UserGroupMapper;
import com.atguigu.userprofile.service.TagInfoService;
import com.atguigu.userprofile.service.UserGroupService;
import com.atguigu.userprofile.utils.RedisUtil;
import com.baomidou.dynamic.datasource.annotation.DS;
import com.baomidou.mybatisplus.extension.service.impl.ServiceImpl;
import lombok.extern.slf4j.Slf4j;

import org.apache.commons.lang3.StringUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import redis.clients.jedis.Jedis;

import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * <p>
 *  服务实现类
 * </p>
 *
 * @author zhangchen
 * @since 2021-05-04
 */
@Service
@Slf4j
@DS("mysql")
public class UserGroupServiceImpl extends ServiceImpl<UserGroupMapper, UserGroup> implements UserGroupService {


    @Autowired
    TagInfoService tagInfoService;

    @Override
    public void genUserGroup(UserGroup userGroup) {
        // 1 写入 mysql 人群的基本定义
        List<TagCondition> tagConditions = userGroup.getTagConditions();
        String conditionJson = JSON.toJSONString(tagConditions);
        userGroup.setConditionJsonStr(conditionJson);
        userGroup.setConditionComment(userGroup.conditionJsonToComment());
        userGroup.setCreateTime(new Date());
        super.saveOrUpdate(userGroup);

        //2 写入 clickhouse  人群包
       // 2.1 组合查询Bitmap表的大SQL
       // 2.2 人群包的建表(手动去clickhouse中建立)
       // 2.3insert into  +查SQL
        //2.4 更新人群包的人数

        // 2.1 组合查询Bitmap表的大SQL
        Map<String, TagInfo> tagInfoMapWithCode = tagInfoService.getTagInfoMapWithCode();
        String bitAndSQL = getBitAndSQL(userGroup.getTagConditions(), tagInfoMapWithCode, userGroup.getBusiDate());
        System.out.println(bitAndSQL);


        // 2.3insert into  +查SQL
        String insertBitSQL = getInsertBitSQL(userGroup.getId().toString(), bitAndSQL);

        // 执行语句  //父类已经装配好了mapper 直接使用即可
        super.baseMapper.insertSQL(insertBitSQL);

        //2.4 更新人群包的人数
        // 2.4.1  从已有人群包中 查询人群个数
        Long userGroupCount = getUserGroupCount(userGroup.getId().toString());
        // 2.4.2  更新 MySQL分群基本信息中的人数  mybatis-plus
        userGroup.setUserGroupNum(userGroupCount);
        super.saveOrUpdate(userGroup);


        //3 人群包(所有的uid)  应对高QPS的访问
        //  redis( bitmap\set)
        // 1 查询出人群包 uids的集合
        List<String> uidList = super.baseMapper.userGroupUidList(userGroup.getId().toString());
        // 2 type?   set   key ?  user_group: 101       value?  uid ...   field score?  无    写api? sadd   读api?  smembers   失效？不是临时值
        // 不设失效
        Jedis jedis = RedisUtil.getJedis();
        String key="user_group:"+userGroup.getId();
        String[] uidArr = uidList.toArray(new String[]{});
        jedis.sadd(key,uidArr );
        jedis.close();


    }

    @Override
    public Long evaluateUserGroup(UserGroup userGroup) {
        // 1 把对象中的筛选条件组合成一个大的SQL  ，用于查询一个临时bitmap集合
        // 2 再用bitmapCardinality 函数把临时的bitmap集合的个数查询出来
        Map<String, TagInfo> tagInfoMapWithCode = tagInfoService.getTagInfoMapWithCode();
        String bitAndSQL = getBitAndSQL(userGroup.getTagConditions(), tagInfoMapWithCode, userGroup.getBusiDate());
        Long userCount = super.baseMapper.evalUserGroup(bitAndSQL);
        return userCount;
    }

    @Override
    public void refreshUserGroup(String userGroupId, String busiDate) {
        //1 先用userGroupId 去查询出userGroup
        UserGroup userGroup = super.getById(userGroupId);
        String conditionJsonStr = userGroup.getConditionJsonStr();
        List<TagCondition> tagConditionList = JSON.parseArray(conditionJsonStr, TagCondition.class);


        //2 写入 clickhouse  人群包
        // 2.1 组合查询Bitmap表的大SQL
        // 2.2 人群包的建表(手动去clickhouse中建立)
        // 2.3insert into  +查SQL
        //2.4 更新人群包的人数

        // 2.1 组合查询Bitmap表的大SQL
        Map<String, TagInfo> tagInfoMapWithCode = tagInfoService.getTagInfoMapWithCode();
        String bitAndSQL = getBitAndSQL(tagConditionList, tagInfoMapWithCode, busiDate);
        System.out.println(bitAndSQL);



        // 2.3insert into  +查SQL
        String insertBitSQL = getInsertBitSQL(userGroup.getId().toString(), bitAndSQL);

        //  更新操作   执行insert 之前 要把原有的人群包删除掉
        // clickhouse 可以删除数据？  要考虑是否会影响同分区的其他数据
        //因为 分群表是用user_group_id进行分区的 一个分群一个分区 所以 删除分群不会影响其他数据
        super.baseMapper.deleteUserGroup(userGroupId);


        // 执行语句  //父类已经装配好了mapper 直接使用即可
        super.baseMapper.insertSQL(insertBitSQL);

        //2.4 更新人群包的人数
        // 2.4.1  从已有人群包中 查询人群个数
        Long userGroupCount = getUserGroupCount(userGroup.getId().toString());
        // 2.4.2  更新 MySQL分群基本信息中的人数  mybatis-plus
        userGroup.setUserGroupNum(userGroupCount);
        super.saveOrUpdate(userGroup);


        //3 人群包(所有的uid)  应对高QPS的访问
        //  redis( bitmap\set)
        // 1 查询出人群包 uids的集合
        List<String> uidList = super.baseMapper.userGroupUidList(userGroup.getId().toString());
        // 2 type?   set   key ?  user_group: 101       value?  uid ...   field score?  无    写api? sadd   读api?  smembers   失效？不是临时值
        // 不设失效
        Jedis jedis = RedisUtil.getJedis();
        String key="user_group:"+userGroup.getId();
        jedis.del(key);
        String[] uidArr = uidList.toArray(new String[]{});
        jedis.sadd(key,uidArr );
        jedis.close();



    }

    public  Long getUserGroupCount(String userGroupId){
        Long userGroupCount = super.baseMapper.userGroupCount(userGroupId);
        return userGroupCount;
    }

    public   String getInsertBitSQL(String userGroupId,String bitAndSQL){
         String insertSQL="insert into user_group  select '"+userGroupId+"' ,"+bitAndSQL+" as  us";
         return insertSQL;
    }


    //获得整个bitmapAnd的SQL
    public  String getBitAndSQL(List<TagCondition> tagConditionList,Map<String,TagInfo> tagInfoMap,String busiDate){
       StringBuilder sqlBuilder=new StringBuilder("");
        for (TagCondition tagCondition : tagConditionList) {
            String conditionSQL = getConditionSQL(tagCondition, tagInfoMap, busiDate);
            if(sqlBuilder.length()==0){
                sqlBuilder.append(conditionSQL) ;
            }else{  //insert 怼前面   append怼后面
                sqlBuilder.insert(0," bitmapAnd(").append(","+conditionSQL+")");

            }
        }

       return   sqlBuilder.toString();


    }


    // 一个筛选条件 生成子查询
    public  String  getConditionSQL(TagCondition tagCondition ,Map<String,TagInfo> tagInfoMap ,String busiDate){
        // 1  标签的值类型
        TagInfo tagInfo = tagInfoMap.get(tagCondition.getTagCode());
        String tagValueType = tagInfo.getTagValueType();
       // 2 确定表名
        String tableName=null;
        if(tagValueType.equals(ConstCodes.TAG_VALUE_TYPE_LONG)){
            tableName="user_tag_value_long";
        }else if(tagValueType.equals(ConstCodes.TAG_VALUE_TYPE_STRING)){
            tableName="user_tag_value_string";
        }else if(tagValueType.equals(ConstCodes.TAG_VALUE_TYPE_DECIMAL)){
            tableName="user_tag_value_decimal";
        }else if(tagValueType.equals(ConstCodes.TAG_VALUE_TYPE_DATE)){
            tableName="user_tag_value_date";
        }
        //值
        String tagValueSQL=getTagValueSQL(tagCondition.getTagValues(),tagValueType);
       // 操作符
        String operatorSQL=getConditionOperator(tagCondition.getOperator());

        String conditionSQL="(select groupBitmapMergeState(us) from  "+tableName+" where tag_code='"+tagCondition.getTagCode()
                +"' and  tag_value "+operatorSQL +" "+tagValueSQL+ " and  dt ='"+busiDate+"')";

        return conditionSQL;

    }


     //获得一个值的sql
    public String getTagValueSQL(List<String> valueList,String tagValueType){
       // List<String> valueWithQouteList = valueList.stream().map(value -> needSingleQuote(value,tagValueType)).collect(Collectors.toList());
        boolean needQoute = needSingleQuote(tagValueType);
        String tagValueSQL=null;
        if(needQoute){
            tagValueSQL= " '"+ StringUtils.join(valueList, "','")+"' ";
        }else{
            tagValueSQL=   StringUtils.join(valueList, ",") ;
        }
        if(valueList.size()>1){
            tagValueSQL=  "("+tagValueSQL+")";
        }
        return tagValueSQL;
    }


    //判断是否需要加单引
    public  boolean needSingleQuote( String tagValueType ){
        if(tagValueType.equals(ConstCodes.TAG_VALUE_TYPE_LONG)||tagValueType.equals(ConstCodes.TAG_VALUE_TYPE_DECIMAL)){
           return false;
        }else  {
            return true;
        }

    }
   //转换英文操作符
    private  String getConditionOperator(String operator){
        switch (operator){
            case "eq":
                return "=";
            case "lte":
                return "<=";
            case "gte":
                return ">=";
            case "lt":
                return "<";
            case "gt":
                return ">";
            case "neq":
                return "<>";
            case "in":
                return "in";
            case "nin":
                return "not in";
        }
        throw  new RuntimeException("操作符不正确");
    }


}
