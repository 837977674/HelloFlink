import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer09;
import org.apache.flink.util.Collector;


import java.util.Date;
import java.util.Properties;
import java.util.concurrent.ConcurrentHashMap;

public class WordCountCpt {
    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        Properties props = new Properties();
        props.setProperty("bootstrap.servers", "cdh2:9092");
        props.setProperty("group.id", "app2");

        FlinkKafkaConsumer09<String> consumer =
                new FlinkKafkaConsumer09<>("wxhznb", new SimpleStringSchema(), props);

        //env.addSource(consumer).flatMap(new Tokenizer()).keyBy(0).sum(1).print();
        //System.out.println(new Date().getTime());
        env.addSource(consumer).flatMap(new Tokenizer1()).keyBy(0).timeWindow(Time.seconds(5)).sum(1).print();
        //env.addSource(consumer).flatMap(new Tokenizer1()).keyBy(0).window(TumblingEventTimeWindows.of(Time.seconds(5))).sum(1).print();
        //env.addSource(consumer).flatMap(new Tokenizer1()).keyBy(0).timeWindowAll(Time.seconds(5)).reduce((s1,s2)=>)
        env.execute("Streaming WordCount");

    }
}


class Tokenizer1 implements FlatMapFunction<String, Tuple2<String, Integer>> {

    ConcurrentHashMap<String,Integer> counter=new ConcurrentHashMap();
    @Override
    public void flatMap(String value, Collector<Tuple2<String, Integer>> out) {
        /*if(!counter.containsKey(value) ) {
            counter.put(value,1);
        }else {
            counter.put(value,counter.get(value)+1);
        }*/

        //out.collect(new Tuple2<>(value.toLowerCase(), counter.get(value)));
        out.collect(new Tuple2<>(value.toLowerCase(), 1));
        //System.out.println(out.collect(new Tuple2<>(value.toLowerCase(), 1)));
    }

}