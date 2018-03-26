package com.yinhai.yhdi.increment.poto;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import com.yinhai.yhdi.common.KyroUtil;
import com.yinhai.yhdi.common.OdiPrp;
import com.yinhai.yhdi.increment.IcrmtEnv;
import com.yinhai.yhdi.increment.entity.FileIndex;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.util.concurrent.LinkedBlockingDeque;

public class IndexQueue {
    private static final LinkedBlockingDeque<FileIndex> msgQueue = new LinkedBlockingDeque();
    private File inFile;
    private File outFile;
    private String indexDir;
    private final Kryo kryo = KyroUtil.getKryo();
    private final static Logger logger = LoggerFactory.getLogger(IndexQueue.class);

    /**
     * init
     */
    public IndexQueue() {
        indexDir = OdiPrp.getProperty("index.path");
        //check index file dir;
        File dirFile = new File(indexDir);
        if (!dirFile.exists()) {
            dirFile.mkdir();
        }
        inFile = new File(indexDir,"msgQueue.on");
        outFile = new File(indexDir,"outPoint");//记录已经取走的位置。
    }

    /**
     * 文件索引加入队列，先持久化，再ADD队列
     * @param fileIndex
     * @throws FileNotFoundException
     */
    public void add(FileIndex fileIndex) throws FileNotFoundException {
        Output op = new Output(new FileOutputStream(inFile,true));
        kryo.writeObject(op, fileIndex);
        op.flush();
        op.close();
        msgQueue.add(fileIndex);

    }

    /**
     * 弹出最前面一个索引，并持久到文件，记录弹出节点。
     * @return
     */
    public FileIndex poll() throws FileNotFoundException {
        FileIndex fileIndex = msgQueue.poll();
        Output op = new Output(new FileOutputStream(outFile));
        kryo.writeObject(op, fileIndex);
        op.flush();
        op.close();
        return fileIndex;
    }

    /**
     * 获取最前面一个索引，只获取，不弹出。
     * @return
     */
    public FileIndex getFirst() {
        return msgQueue.getFirst();
    }

    /**
     * 启动源端进程时调用：读取持久化文件来初始化索引队列。
     */
    public void startup() throws IOException {
        msgQueue.clear();
        FileIndex fileIndex = null;
        if (inFile.exists()) {//索引文件还为生成，为首次启动
            Input input = new Input(new FileInputStream(inFile));
            while (input.available() != 0) {
                fileIndex = kryo.readObject(input, FileIndex.class);
                msgQueue.add(fileIndex);
            }
            input.close();
        }else {
            logger.warn("未找到索引队列文件，此次启动按首次启动规则启动。");
        }
        //最后一个索引及为上次抽取的断点，放入到ENV中。
        if (fileIndex != null) {
            IcrmtEnv.setLastIndex(fileIndex);
        }else {
            FileIndex fileIndex1 = new FileIndex();
            fileIndex1.setScn(0L);
            fileIndex1.setRsid("");
            fileIndex1.setSsn(0);
            IcrmtEnv.setLastIndex(fileIndex1);//首次启动时放入这个。
        }

    }

    /**
     * 必须保证没有新的Fileindex写入队列时，才能调用此方法。
     * 关闭源端进程时调用：根据outPoint位置，重新持久化msgQueue,以便减小文件大小，
     * 缩短下次启动时初始化队列的时间。
     * 清理时，最少要保证还剩下1个索引，不能全部清空。
     */
    public void shutdown() {

    }
}
