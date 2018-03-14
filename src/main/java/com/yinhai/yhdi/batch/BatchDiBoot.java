package com.yinhai.yhdi.batch;

/**
 * 数据抽取入口类
 * @author win-leejie
 */
public class BatchDiBoot {
    public static void main(String[] args) {
        int len = args.length;
        String stable ;
        String datapath ;
        BatchDiDriver batchDiDriver;
        int parallel = 1;
        if (len == 3) {
            stable=args[0];//抽取表
            datapath=args[1];//存放目录
            parallel=Integer.valueOf(args[2]);//并行度
            batchDiDriver = new BatchDiDriver(stable,datapath,parallel);
        }else {
            batchDiDriver = new BatchDiDriver();
        }
        //执行抽取
        try {
            batchDiDriver.startExtract();
        } catch (Exception e) {
            e.printStackTrace();
        }


    }
}
