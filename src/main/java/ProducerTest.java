import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer09;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.Random;

public class ProducerTest {
    public static void main(String[] args) {


        Properties kafkaProps = new Properties();
        kafkaProps.put("bootstrap.servers", "cdh2:9092,cdh3:9092,cdh4:9092");
        kafkaProps.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        kafkaProps.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");


// 步骤2 创建新的生产者对象 KafkaProducer ，并把设置好的 Properties 对象传给它
        //FlinkKafkaProducer09<String> producer= new FlinkKafkaProducer09<>("wxhznb",
         //       new SimpleStringSchema(),
          //      kafkaProps);
        // 步骤3 开始发送消息，生产者将 ProducerRecord 对象作为参数发送，使用send() 方法发送
        //ProducerRecord<String, String> record = new ProducerRecord<>("wxhznb", "", "");
        //Future<RecordMetadata> kafkaFuture = producer.send(record);

        Producer<String, String> producer = new KafkaProducer<>(kafkaProps);
        Random r = new Random();
        int i=0;
        try {
            while (true) {
                String[] candidate = new String[]{"aaa", "bbb", "ccc"};
                int idx = Math.abs(r.nextInt()) % 3;
                String txt = candidate[idx];

                Map<String,Integer> map1=new HashMap<>();
                int value= r.nextInt(20);
                map1.put(txt,value);
                String str=txt+","+value;
                //System.out.println(str);


                ProducerRecord record = new ProducerRecord<String, String>(
                        "wxhznb",
                        null,
                        null,
                        str);
                producer.send(record);
                System.out.println(str);
                i=i+1;
                if (i/5%2==0)
                    Thread.sleep(1000);
                else Thread.sleep(500);

            }
        }catch (Exception e){
            e.printStackTrace();
        }

    }
}

