package com.xkj.mlrc.clean.util;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Properties;

/**
 * 获取系统配置的工具类
 *
 * @author lijf@2345.com
 * @return
 */
public final class PropsUtil {

    private static Properties props = null;
    private static Logger logger = LoggerFactory.getLogger(PropsUtil.class);

    static {
        try {
            props = new Properties();
            props.load(PropsUtil.class.getClassLoader().getResourceAsStream("config.properties"));
        } catch (IOException e) {
            logger.error(e.toString());
        }
    }

    private PropsUtil() {
    }

    /**
     * 获取配置
     *
     * @param key key
     * @return value
     */
    public static String getProp(String key) {
        if (props != null) {
            return props.getProperty(key);
        }
        return null;
    }


}
