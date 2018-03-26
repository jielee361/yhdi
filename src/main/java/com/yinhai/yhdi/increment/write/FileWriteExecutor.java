package com.yinhai.yhdi.increment.write;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Output;
import com.yinhai.yhdi.common.KyroUtil;
import com.yinhai.yhdi.common.OdiPrp;
import com.yinhai.yhdi.increment.IcrmtEnv;
import com.yinhai.yhdi.increment.entity.FileIndex;
import com.yinhai.yhdi.increment.entity.IcrmtConf;
import com.yinhai.yhdi.increment.poto.IndexQueue;
import com.yinhai.yhdi.increment.poto.RedoObj;
import com.yinhai.yhdi.increment.poto.SqlPoto;
import com.yinhai.yhdi.increment.parser.OraFileParser;

import java.io.File;
import java.io.FileOutputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.LinkedBlockingDeque;

public class FileWriteExecutor extends WriteExecutor {
    private boolean stopFlag;
    private int fileSize;
    private IcrmtConf icrmtConf;
    public FileWriteExecutor() {
        this.icrmtConf = IcrmtEnv.getIcrmtConf();
        this.fileSize = icrmtConf.getFileSize();
        this.stopFlag = false;
    }
    @Override
    public void startWrite(LinkedBlockingDeque<RedoObj> redoQueue) throws Exception {
        int readNum = 0;
        long scn = 0L;
        String rsid = "";
        int ssn = 0;
        String fileName;
        List<SqlPoto> sqlPotoList = new ArrayList<>();
        OraFileParser oraFileParser = new OraFileParser();
        String dataPath = OdiPrp.getProperty("data.path");
        if (!new File(dataPath).exists()) {
            new File(dataPath).mkdir();
        }
        IndexQueue indexQueue = IcrmtEnv.getIndexQueue();
        Kryo kryo = KyroUtil.getKryo();

        while (!stopFlag) {
            //begin to read data from queue
            while (readNum <  fileSize) {
                RedoObj redoObj = redoQueue.poll();
                if (redoObj == null) {
                    break;
                }
                //parse sql
                sqlPotoList.add(oraFileParser.redo2Poto(redoObj));
                scn = redoObj.getScn();
                rsid = redoObj.getRs_id();
                ssn = redoObj.getSsn();
                readNum ++;

            }
            if (readNum == 0) {
                Thread.sleep(icrmtConf.getPauseTime());
                continue;
            }
            //write to file
            fileName = new StringBuffer().append(scn).append("-").append(rsid).append("-").append(ssn).toString();
            File sqlFile = new File(dataPath,fileName);
            Output op = new Output(new FileOutputStream(sqlFile));
            kryo.writeObject(op, sqlPotoList);
            op.flush();
            op.close();
            //write message queue
            FileIndex fileIndex = new FileIndex();
            fileIndex.setScn(scn);
            fileIndex.setRsid(rsid);
            fileIndex.setSsn(ssn);
            indexQueue.add(fileIndex);

            if (readNum < fileSize) {
                Thread.sleep(2000);
            }

        }

    }

    @Override
    public void stopWrite() {
        this.stopFlag = true;

    }
}
