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
        String confPath = cfile.getAbsolutePath()+File.separator+"conf"+File.separator;
        String dataPath = cfile.getAbsolutePath()+File.separator+"data"+File.separator;
        String indexPath = cfile.getAbsolutePath()+File.separator+"index"+File.separator;
        //加载log4j配置文件
        try {
            PropertyConfigurator.configure(confPath+"log4j.properties");
            System.out.println("[INFO ] LOG4J配置文件加载成功！");
        }catch (Exception e) {
            System.out.println("[ERROR] 加载LOG4J配置文件失败！" );
            e.printStackTrace();
        }
        //加载配置项目配置文件
        prp = new Properties();
        String file1 = confPath + "icrmt-odi.properties";
        String file2 = confPath + "batch-odi.properties";
        String file3 = confPath + "increment.properties";
        prp.put("data.path",dataPath);//数据文件存放目录
        prp.put("index.path",indexPath);//索引文件存放目录
        try {
            if (new File(file1).exists()) {
                BufferedInputStream bs1 = new BufferedInputStream(new FileInputStream(file1));
                prp.load(bs1);
                bs1.close();
            }else {
                logger.warn("未找到配置文件：" + file1);
            }
            if (new File(file2).exists()) {
                BufferedInputStream bs2 = new BufferedInputStream(new FileInputStream(file2));
                prp.load(bs2);
                bs2.close();
            }else {
                logger.warn("未找到配置文件：" + file2);
            }
            if (new File(file3).exists()) {
                BufferedInputStream bs3 = new BufferedInputStream(new FileInputStream(file3));
                prp.load(bs3);
                bs3.close();
            }else {
                logger.warn("未找到配置文件：" + file3);
            }
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
        if (prp.getProperty(key) == null) {
            throw new RuntimeException("未找到配置项目：" + key);
        }
        return Long.valueOf(prp.getProperty(key));
    }


}
