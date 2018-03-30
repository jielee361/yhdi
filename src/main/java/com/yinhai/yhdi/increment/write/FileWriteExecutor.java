package com.yinhai.yhdi.increment.write;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Output;
import com.yinhai.yhdi.common.KryoUtil;
import com.yinhai.yhdi.common.DiPrp;
import com.yinhai.yhdi.increment.IcrmtEnv;
import com.yinhai.yhdi.increment.entity.FileIndex;
import com.yinhai.yhdi.increment.entity.IcrmtConf;
import com.yinhai.yhdi.increment.poto.IndexQueue;
import com.yinhai.yhdi.increment.poto.RedoObj;
import com.yinhai.yhdi.increment.poto.SqlPoto;
import com.yinhai.yhdi.increment.parser.OraFileParser;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileOutputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.LinkedBlockingDeque;

public class FileWriteExecutor extends WriteExecutor {
    private boolean stopFlag;
    private int fileSize;
    private IcrmtConf icrmtConf;
    private final static Logger logger = LoggerFactory.getLogger(FileWriteExecutor.class);
    public FileWriteExecutor() {
        this.icrmtConf = IcrmtEnv.getIcrmtConf();
        this.fileSize = icrmtConf.getFileSize();
        this.stopFlag = false;
    }
    @Override
    public void startWrite(LinkedBlockingDeque<RedoObj> redoQueue) throws Exception {
        initPkMap();//获取主键信息
        int readNum = 0;
        long scn = 0L;
        String rsid = "";
        int ssn = 0;
        String fileName;
        List<SqlPoto> sqlPotoList = new ArrayList<>();
        OraFileParser oraFileParser = new OraFileParser(this.pkMap);
        String dataPath = DiPrp.getProperty("data.path");
        IndexQueue indexQueue = IcrmtEnv.getIndexQueue();
        Kryo kryo = KryoUtil.getKryo();

        while (!stopFlag) {
            //begin to read data from queue
            readNum = 0;
            sqlPotoList.clear();
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
            FileIndex fileIndex = new FileIndex();
            fileIndex.setScn(scn);
            fileIndex.setRsid(rsid);
            fileIndex.setSsn(ssn);
            //write to file
            File sqlFile = new File(dataPath,fileIndex.toString());
            if (sqlFile.exists()) {
                sqlFile.delete();
            }
            Output op = new Output(new FileOutputStream(sqlFile));
            kryo.writeObject(op, sqlPotoList);
            op.flush();
            op.close();
            //write message queue
            indexQueue.add(fileIndex);
            logger.debug("read-write-num: " + readNum);

            if (readNum < fileSize) {
                Thread.sleep(2000);
            }

        }

    }

}
