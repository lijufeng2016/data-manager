package com.xkj.mlrc.fsimage;


import org.apache.commons.io.FileUtils;
import org.apache.hadoop.fs.Path;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.net.URL;
import java.util.List;

/**
 * @author lijf@2345.com
 * @date 2020/4/23 11:56
 * @desc
 */

public class GetFromFsImageTest {


    @Test
    public void getAllFiles() throws IOException {
        ClassLoader classLoader = GetFromFsImageTest.class.getClassLoader();
        /**
         getResource()方法会去classpath下找这个文件，获取到url resource, 得到这个资源后，调用url.getFile获取到 文件 的绝对路径
         */
        URL url = classLoader.getResource("exclusion_tables.txt");
        /**
         * url.getFile() 得到这个文件的绝对路径
         */
        System.out.println(url.getFile());
        File file = new File(url.getFile());
        System.out.println(file.exists());
        List<String> exclusionTables = FileUtils.readLines(file);

        String s = "fds/fdf/hfhgf/hjgf";
        Path path = new Path(s);
        System.out.println(path.getName());
    }


}
