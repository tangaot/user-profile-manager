package com.atguigu.userprofile.mapper;

import com.atguigu.userprofile.bean.UserGroup;
import com.baomidou.dynamic.datasource.annotation.DS;
import com.baomidou.mybatisplus.core.mapper.BaseMapper;
import org.apache.ibatis.annotations.*;
import org.springframework.stereotype.Service;

import javax.ws.rs.DELETE;
import java.util.List;

/**
 * <p>
 *  Mapper 接口
 * </p>
 *
 * @author zhangchen
 * @since 2021-05-04
 */


@Mapper
@DS("mysql")
public interface UserGroupMapper extends BaseMapper<UserGroup> {

    @Insert("${sql}")
    @DS("clickhouse")
    public  void  insertSQL( @Param("sql") String sql);

    @Select("select   bitmapCardinality( us)  ct from  user_group  where  user_group_id=#{userGroupId}")
    @DS("clickhouse")
    public Long  userGroupCount(@Param("userGroupId") String userGroupId);

    @Select("select   bitmapCardinality( ${bitandSQL})   ")
    @DS("clickhouse")
    public Long  evalUserGroup (@Param("bitandSQL") String bitandSQL  );

    @Delete("alter table user_group delete where user_group_id=  #{userGroupId}  ")
    @DS("clickhouse")
    public void  deleteUserGroup (@Param("userGroupId") String userGroupId  );

    @Select("select     arrayJoin(  bitmapToArray( us) )  as us  from  user_group  where  user_group_id=#{userGroupId}")
    @DS("clickhouse")
    public  List<String>  userGroupUidList(@Param("userGroupId") String userGroupId);

}
