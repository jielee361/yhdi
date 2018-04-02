package com.yinhai.yhdi.increment.poto;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import com.yinhai.yhdi.common.KryoUtil;
import com.yinhai.yhdi.common.DiPrp;
import com.yinhai.yhdi.increment.IcrmtEnv;
import com.yinhai.yhdi.increment.entity.FileIndex;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.util.concurrent.LinkedBlockingDeque;

public class IndexQueue {
    private static final LinkedBlockingDeque<FileIndex> msgQueue = new LinkedBlockingDeque<>();
    private File inFile;
    private File outFile;
    private String indexDir;
    private final Kryo kryo = KryoUtil.getKryo();
    private final static Logger logger = LoggerFactory.getLogger(IndexQueue.class);

    /**
     * init
     */
    public IndexQueue() {
        indexDir = DiPrp.getProperty("index.path");//只从配置文件读取
        inFile = new File(indexDir,"msgQueue.on");//索引队列
        outFile = new File(indexDir,"outPoint");//记录已经取走的位置。
    }
    public int getSize() {
        return msgQueue.size();
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
     * 弹出最前面一个索引，并持久到标记文件，记录弹出节点。
     * @return
     */
    public FileIndex poll() throws FileNotFoundException {
        Output op = new Output(new FileOutputStream(outFile));
        kryo.writeObject(op, msgQueue.getFirst());
        op.flush();
        op.close();
        return msgQueue.poll();//若写入成功，弹出失败，会有风险。几率很小
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
        FileIndex fileIndexIn = null;
        FileIndex fileIndexOut = null;
        boolean isout = false;//是否是已经获取过的索引标记。
        //获取上次处理节点
        if (outFile.exists()) {
            Input inputOut = new Input(new FileInputStream(outFile));
            fileIndexOut = kryo.readObject(inputOut, FileIndex.class);
            inputOut.close();
            isout = true;
        }
        //循环读取未处理的索引到msgQueue中
        File tmpFile = new File(indexDir,"msgQueue.tmp");//临时队列
        if (tmpFile.exists()) {
            tmpFile.delete();
        }
        Output tmpOp = new Output(new FileOutputStream(tmpFile,true));
        int queueLenth = 0;
        if (inFile.exists()) {
            Input inputIn = new Input(new FileInputStream(inFile));
            while (inputIn.available() != 0) {
                fileIndexIn = kryo.readObject(inputIn, FileIndex.class);
                if (isout) {
                    if (fileIndexIn.toString().equals(fileIndexOut.toString())) {
                        isout = false;//此后的索引都是未获取过的索引，需要放入到索引队列中。
                        kryo.writeObject(tmpOp, fileIndexIn);//最少文件里要保持一个索引
                    }

                }else {
                    msgQueue.add(fileIndexIn);
                    kryo.writeObject(tmpOp, fileIndexIn);
                    queueLenth ++;
                }
            }
            inputIn.close();
            tmpOp.close();
            //转换存储文件
            File offFile = new File(indexDir,"msgQueue.off");
            if (offFile.exists()) {
                offFile.delete();
            }
            inFile.renameTo(offFile);
            tmpFile.renameTo(inFile);
            inFile = new File(indexDir,"msgQueue.on");
        }else {
            logger.warn("未找到索引队列文件，此次启动按首次启动规则启动。");
        }

        //最后一个索引及为上次抽取的断点，放入到ENV中。
        if (fileIndexIn != null) {
            IcrmtEnv.setLastIndex(fileIndexIn);
            logger.info("索引队列装载完成，上次最后抽取的节点是：" + fileIndexIn.toString());
        }else {//索引文件还为生成，为首次启动
            FileIndex fileIndex1 = new FileIndex();
            fileIndex1.setScn(0L);
            fileIndex1.setRsid("");
            fileIndex1.setSsn(0);
            IcrmtEnv.setLastIndex(fileIndex1);//首次启动时放入这个。
        }

    }

}
