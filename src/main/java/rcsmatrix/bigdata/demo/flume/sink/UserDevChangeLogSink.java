package rcsmatrix.bigdata.demo.flume.sink;

import kafka.javaapi.producer.Producer;
import kafka.producer.KeyedMessage;
import kafka.producer.ProducerConfig;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.flume.*;
import org.apache.flume.conf.Configurable;
import org.apache.flume.sink.AbstractSink;

import java.util.Properties;
import java.util.Random;


public class UserDevChangeLogSink extends AbstractSink implements Configurable {
    private static final Log logger = LogFactory.getLog(BossAgentLogSink.class);

    private static final String topic = "user_dev_change_log_topic";
    private static final int numPartitions = 4;
    private Producer<String, String> producer;
    private Random ran = new Random();

    /**
     * 配置Kafka生产者
     */
    public void configure(Context context) {
        Properties props = new Properties();
        //配置metadata.broker.list, 指定kafka节点列表，用于获取metadata
        props.put("metadata.broker.list", "hadoop-01:9092,hadoop-02:9092,hadoop-03:9092");
        props.put("zookeeper.connect", "hadoop-01:2181,hadoop-02:2181,hadoop-03:2181");
        //serializer.class为消息的序列化类
        props.put("serializer.class", "kafka.serializer.StringEncoder");
        //指定分区处理类。默认kafka.producer.DefaultPartitioner，表通过key哈希到对应分区
        //props.put("partitioner.class", "KafkaPartitioner");
        //每个topic的分区个数
        props.put("num.partitions", numPartitions);
        //ACK机制, 消息发送需要kafka服务端确认
        //producer接收消息ack的时机.默认为0.
        //0: producer不会等待broker发送ack
        //1: 当leader接收到消息之后发送ack
        //-1: 当所有的follower都同步消息成功后发送ack.
        props.put("request.required.acks", "1");
        props.put("producer.type", "async");//异步模式
        props.put("queue.buffering.max.messages", "1000");//在异步模式下，producer端允许buffer的最大消息数量.默认值：10000
        props.put("queue.buffering.max.ms", "100");//默认值：5000
        props.put("batch.num.messages", "500");//默认值：200
//        props.put("queue.buffering.max.messages", "10000000");//在异步模式下，producer端允许buffer的最大消息数量.默认值：10000
//        props.put("queue.buffering.max.ms", "200");//默认值：5000
//        props.put("batch.num.messages", "1000000");//默认值：200
        ProducerConfig config = new ProducerConfig(props);
        producer = new Producer<String, String>(config);
        logger.info("UserDevChangeLogSink初始化完成.");

    }

    public Status process() throws EventDeliveryException {
        Channel channel = getChannel();
        Transaction tx = channel.getTransaction();
        try {
            tx.begin();
            Event e = channel.take();
            if (e == null) {
                tx.rollback();
                return Status.BACKOFF;
            }
            KeyedMessage<String, String> data = new KeyedMessage<String, String>(topic, String.valueOf(ran.nextInt(numPartitions)),new String(e.getBody()));
            producer.send(data);
            //logger.info("flume向kafka发送消息：" + new String(e.getBody()));
            tx.commit();
            return Status.READY;
        } catch (Exception e) {
            logger.error("Flume UserDevChangeLogSinkException:", e);
            tx.rollback();
            return Status.BACKOFF;
        } finally {
            tx.close();
        }
    }
    /**
     * Stop void.
     */
    @Override
    public void stop() {
        producer.close();
    }
}