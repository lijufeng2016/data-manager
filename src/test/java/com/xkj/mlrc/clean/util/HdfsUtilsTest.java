package com.xkj.mlrc.clean.util;


import org.junit.Test;
import org.junit.runner.RunWith;

import java.io.IOException;

import static org.junit.Assert.*;

/**
 * @author lijf@2345.com
 * @date 2020/4/23 13:30
 * @desc
 */

public class HdfsUtilsTest {


    @Test
    public void movefile() throws IOException {
        HdfsUtils.movefile("/user/lijf/2019-01-03/zhangyong.log",".Trash/user/lijf/2019-01-03/zhangyong.log");

    }

    @Test
    public void delete() throws IOException {
        System.setProperty("HADOOP_USER_NAME", "lijf");

           HdfsUtils.delete("/user/lijf/2019-01-03");
    }
}
