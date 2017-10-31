package rcsmatrix.bigdata.demo.flume.sink;

import kafka.producer.Partitioner;
import kafka.utils.VerifiableProperties;


public class KafkaPartitioner<T> implements Partitioner {
	public KafkaPartitioner(VerifiableProperties props) {

	}
	//private static final Log logger = LogFactory.getLog(KafkaPartitioner.class);
	public int partition(Object key, int numPartitions) {
		//int i = Math.abs(key.hashCode()) % numPartitions;
		//logger.info("key is "+ key+ " numPartitions is "+ numPartitions+ " partitions is "+ i);
		//return i;
		return Math.abs(key.hashCode()) % numPartitions;
	}
}
