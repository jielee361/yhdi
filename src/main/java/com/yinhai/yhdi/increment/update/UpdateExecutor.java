package com.yinhai.yhdi.increment.update;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Input;
import com.yinhai.yhdi.common.KryoUtil;
import com.yinhai.yhdi.common.DiPrp;
import com.yinhai.yhdi.increment.IcrmtEnv;
import com.yinhai.yhdi.increment.entity.FileIndex;
import com.yinhai.yhdi.increment.entity.IcrmtConf;
import com.yinhai.yhdi.increment.poto.IndexQueue;
import com.yinhai.yhdi.increment.poto.SqlPoto;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.util.ArrayList;

public abstract class UpdateExecutor {
    private String dataDir;
    private IndexQueue indexQueue;
    private Kryo kryo;
    boolean stopFlag;
    IcrmtConf icrmtConf;
    private FileIndex pollIndex;
    public UpdateExecutor() {
        this.dataDir = DiPrp.getProperty("data.path");
        this.indexQueue = IcrmtEnv.getIndexQueue();
        this.kryo = KryoUtil.getKryo();
        stopFlag = false;
        icrmtConf = IcrmtEnv.getIcrmtConf();
    }
    public abstract void startUpdate() throws Exception;
    public  void stopUpdate() {
        stopFlag = true;
    }

    ArrayList<SqlPoto> readNext() throws FileNotFoundException {
        //get index
        if (indexQueue.getSize() == 0) {
            return null;
        }
        FileIndex nextIndex = indexQueue.getFirst();
        //read file
        File file = new File(dataDir,nextIndex.toString());
        Input input = new Input(new FileInputStream(file));
        ArrayList<SqlPoto> arrayList = kryo.readObject(input, ArrayList.class);
        input.close();
        return arrayList;
    }

    void pollNext() throws FileNotFoundException {
        try {
            pollIndex= indexQueue.poll();
        }catch (Exception e) {
            e.printStackTrace();
            throw new RuntimeException("重要错误：目标已更新成功，但写入标记文件失败！下次启动必须剔除掉队列中的第一个文件！");
        }
        //delete file
        File file = new File(dataDir,pollIndex.toString());
        file.delete();
    }
}
