package com.yinhai.yhdi.increment.write;

import com.alibaba.fastjson.JSONObject;
import com.yinhai.yhdi.common.DiPrp;
import com.yinhai.yhdi.increment.IcrmtEnv;
import com.yinhai.yhdi.increment.entity.FileIndex;
import com.yinhai.yhdi.increment.parser.OraSqlParser;
import com.yinhai.yhdi.increment.poto.IndexQueue;
import com.yinhai.yhdi.increment.poto.RedoObj;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;
import java.util.concurrent.LinkedBlockingDeque;

public class KafkaWriteExecutor extends WriteExecutor {
    private final static Logger logger = LoggerFactory.getLogger(KafkaWriteExecutor.class);
    @Override
    public void startWrite(LinkedBlockingDeque<RedoObj> redoQueue) throws Exception {
        //initPkMap();//获取主键信息
        String topic= DiPrp.getProperty("kafka.topic");
        String kafkaUrl = DiPrp.getProperty("kafka.url");
        Properties props = new Properties();
        props.put("bootstrap.servers", kafkaUrl);
        props.put("acks", "all");
        props.put("retries", 0);
        props.put("batch.size", 16384);
        props.put("linger.ms", 1);
        props.put("buffer.memory", 33554432);
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

        Producer<String, String> producer = new KafkaProducer<>(props);

        JSONObject jsonObject;
        RedoObj redoObj;
        OraSqlParser oraSqlParser = new OraSqlParser();
        IndexQueue indexQueue = IcrmtEnv.getIndexQueue();
        FileIndex fileIndex;

        while (!stopFlag) {
            redoObj = redoQueue.poll();
            if (redoObj == null) {
                Thread.sleep(1000);
                continue;
            }
            fileIndex = new FileIndex();
            fileIndex.setScn(redoObj.getScn());
            fileIndex.setRsid(redoObj.getRs_id());
            fileIndex.setSsn(redoObj.getSsn());
            jsonObject = oraSqlParser.redo2Json(redoObj);
            producer.send(new ProducerRecord<>(topic,redoObj.getTable_name(),jsonObject.toJSONString())).get();
            logger.info(jsonObject.toJSONString());
            try {
                indexQueue.addK(fileIndex);
            }catch (Exception e) {
                logger.error("kafa队列已发送，但记录节点出错！下次启动可能出现重复");
                throw e;
            }
        }
        producer.close();


    }



    @Override
    public void stopWrite() {
        stopFlag = true;

    }
}
