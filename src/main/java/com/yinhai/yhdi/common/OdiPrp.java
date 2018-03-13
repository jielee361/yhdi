package com.yinhai.yhdi.common;

import java.io.*;
import java.util.Properties;

public class OdiPrp {
    private static Properties prp = null;

    static {
        prp = new Properties();
        File cfile = new File("");
        String cpath = cfile.getAbsolutePath();
        try {
            BufferedInputStream bs = new BufferedInputStream(new FileInputStream(cpath + "/icrmt-odi.properties"));
            BufferedInputStream bs1 = new BufferedInputStream(new FileInputStream(cpath + "/batch-odi.properties"));
            prp.load(bs);
            prp.load(bs1);
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public static String getProperty(String key) {
        return prp.getProperty(key);

    }

    public static Integer getIntProperty(String key) {
        return Integer.valueOf(prp.getProperty(key));
    }


}
