package com.yinhai.yhdi.increment;

import com.yinhai.yhdi.common.CommonConn;
import com.yinhai.yhdi.common.DiPrp;
import com.yinhai.yhdi.common.DiUtil;

import java.sql.Connection;
import java.sql.SQLException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;

public class CarbonSync {
    private static final SimpleDateFormat sdf  = new SimpleDateFormat("yyyy-MM-dd HH:MM:ss");
    public static void main(String[] args) {

        //获取要同步的表，创建它的增量表if not exist

        while (true) {
            System.out.println("==本次更新执行开始 "+sdf.format(new Date()));
            //得到本次应更新到的节点
            long ctimeM = System.currentTimeMillis();
            long endTimeM = ctimeM - DiPrp.getIntProperty("update.delay")*60000;
            String endTime = sdf.format(new Date(endTimeM))+".000000";
            //启动本次更新
            try {
                runCarbonSync(endTime);
            } catch (Exception e) {
                e.printStackTrace();
                break;
            }
            System.out.println("==本次更新执行结束 "+sdf.format(new Date()));
            try {
                Thread.sleep(DiPrp.getIntProperty("run.cycle"));
            } catch (InterruptedException e) {
                e.printStackTrace();
                break;
            }

        }


    }

    private static void  runCarbonSync(String endTime) throws SQLException, ParseException {
        TableUpdateStat updteStat = new TableUpdateStat();
        IcrmtHiveToCarbon hiveToCarbon = new IcrmtHiveToCarbon();
        //获取carbon连接.
        String url = DiPrp.getProperty("carbon.url");
        Connection conn = CommonConn.getHiveConnection(url, "", "");
        //获取要同步的表
        IcrmtTable[] syncTables =  getSyncTables();
        //循环执行更新
        for (int i=0;i<syncTables.length;i++) {
            //获取上次更新节点
            String tabmeName = syncTables[i].getCarbonTable();
            IcrmtTable lastUpdate = updteStat.getLastUpdate(conn, tabmeName);
            if (DiUtil.isEmpty(lastUpdate.getEndTime())) {//第一次更新
                syncTables[i].setBeginTime(DiPrp.getProperty("update.btime"));
            }else {
                if (!isOutofCycle(lastUpdate.getEndTime())) {//如果没有到配置周期，跳出。
                    System.out.println("===本次表："+tabmeName+" 还未达到更新周期，跳出！上次更新节点："
                            +lastUpdate.getEndTime());
                    continue;
                }
                syncTables[i].setBeginTime(lastUpdate.getEndTime());
            }
            syncTables[i].setEndTime(endTime);
            //执行更新
            System.out.println("===开始更新表："+tabmeName +"" +sdf.format(new Date()));
            hiveToCarbon.updateDataToCarbon(syncTables[i]);
            System.out.println("===更新完成表："+tabmeName +"" +sdf.format(new Date()));
            //插入更新节点
            try {
                updteStat.addUpdate(conn,syncTables[i]);
            }catch (Exception e) {
                System.out.println("数据已更新成功，但是标记插入出错，请手动插入标记！\n"+syncTables[i].toString());
                throw new RuntimeException(e);

            }

        }
    }

    private static IcrmtTable[] getSyncTables() {
        //获取要同步的表
        String tables = DiPrp.getProperty("tables");
        String hivedb = DiPrp.getProperty("hive.dbname")+".";
        String[] tableArry = tables.split("[|]");
        int lln = tableArry.length;
        if (DiUtil.isEmpty(tableArry[lln-1])) {
            lln = lln - 1;
        }
        if (lln == 0) {
            throw new RuntimeException("未正确配置同步表名!");
        }
        IcrmtTable[] icrmtTables = new IcrmtTable[lln];
        for (int i=0;i<lln;i++) {
            String itable = tableArry[i];
            String[] tableInfo = itable.split("-");
            String carbonTable = tableInfo[0];
            IcrmtTable icrmtTable = new IcrmtTable();
            icrmtTable.setCarbonTable(carbonTable);
            icrmtTable.setPk(tableInfo[1]);
            icrmtTable.setHiveTable(hivedb+carbonTable+"_c");
            icrmtTables[i] = icrmtTable;
        }
        return icrmtTables;

    }

    private static boolean isOutofCycle(String endTime) throws ParseException {
        Date endDate = sdf.parse(endTime.substring(0, 19));
        int cycleM = DiPrp.getIntProperty("update.cycle")*60000;
        int delayM = DiPrp.getIntProperty("update.delay")*60000;
        if (System.currentTimeMillis() - endDate.getTime() > cycleM + delayM) {
            return true;
        }else {
            return false;
        }

    }
}
