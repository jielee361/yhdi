package com.yinhai.yhdi.common;

import org.apache.log4j.PropertyConfigurator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.util.Properties;

public class OdiPrp {
    private static Properties prp = null;
    private static final Logger logger = LoggerFactory.getLogger(OdiPrp.class);

    static {
        File cfile = new File("");
        String cpath = cfile.getAbsolutePath()+File.separator+"conf"+File.separator;
        //加载log4j配置文件
        try {
            PropertyConfigurator.configure(cpath+"log4j.properties");
            System.out.println("[INFO ] LOG4J配置文件加载成功！");
        }catch (Exception e) {
            System.out.println("[ERROR] 加载LOG4J配置文件失败！" );
            e.printStackTrace();
        }
        //加载配置项目配置文件
        prp = new Properties();
        try {
            BufferedInputStream bs1 = new BufferedInputStream(new FileInputStream(cpath + "icrmt-odi.properties"));
            BufferedInputStream bs2 = new BufferedInputStream(new FileInputStream(cpath + "batch-odi.properties"));
            BufferedInputStream bs3 = new BufferedInputStream(new FileInputStream(cpath + "increment.properties"));
            prp.load(bs1);
            prp.load(bs2);
            prp.load(bs3);
            bs1.close();
            bs2.close();
            bs3.close();
            logger.info("项目配置文件加载成功！");
        } catch (FileNotFoundException e) {
            logger.error("未找到配置文件！");
            e.printStackTrace();
        } catch (IOException e) {
            logger.error("配置文件读取错误！");
            e.printStackTrace();
        }


    }

    public static String getProperty(String key) {
        return prp.getProperty(key);

    }

    public static Integer getIntProperty(String key) {
        return Integer.valueOf(prp.getProperty(key));
    }

    public static long getLongProperty(String key) {
        return Long.valueOf(prp.getProperty(key));
    }


}
