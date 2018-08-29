package com.yinhai.yhdi.common;

import java.io.BufferedReader;
import java.io.InputStream;
import java.io.InputStreamReader;

/**
 * java Runtime操作类，通过Runtime执行命令。
 *
 * @author win-leejie
 * 2017/4/21
 */
public class RuntimeOperate {
    /**
     * 执行命令方法，
     *
     * @param cmd 命令内容字符串（如果有多条命令，请用 &&分隔）
     * @throws Exception
     */
    public static void runCmd(String cmd) throws Exception {
        String[] cmdArray = new String[]{"/bin/sh", "-c", "source ~/.bash_profile &&" + cmd};
        System.out.println("Runtime-run:" + cmd);
        Process process = Runtime.getRuntime().exec(cmdArray);
        InputStream inPrint = process.getInputStream();
        InputStream inError = process.getErrorStream();
        BufferedReader readPrint = new BufferedReader(new InputStreamReader(inPrint));
        BufferedReader readErrot = new BufferedReader(new InputStreamReader(inError));
        String printLog = "";
        while ((printLog = readPrint.readLine()) != null) {
            System.out.println("Runtime-out:" + printLog);
        }
        String errorLog = "";
        String errorLine = "";
        while ((errorLine = readErrot.readLine()) != null) {
            errorLog = errorLog + errorLine + "\n";
        }

        int exitValue = process.waitFor();
        if (exitValue != 0) {
            throw new Exception("Runtime执行命令:\n" + cmd + " 出错：" + errorLog);
        }

    }

}

