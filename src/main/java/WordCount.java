/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */


import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer09;
import org.apache.flink.util.Collector;


import java.util.Properties;
import java.util.concurrent.ConcurrentHashMap;

public class WordCount {

    // *************************************************************************
    // PROGRAM
    // *************************************************************************

    public static void main(String[] args) throws Exception {

        // Checking input parameters
        //final MultipleParameterTool params = MultipleParameterTool.fromArgs(args);

        // set up the execution environment
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // make parameters available in the web interface
        //env.getConfig().setGlobalJobParameters(params);

        Properties props = new Properties();
        props.setProperty("bootstrap.servers", "cdh2:9092");
        props.setProperty("group.id", "app2");

        FlinkKafkaConsumer09<String> consumer =
                new FlinkKafkaConsumer09<>("wxhznb", new SimpleStringSchema(), props);

        env.addSource(consumer).flatMap(new Tokenizer()).keyBy(0).sum(1).print();
        //env.addSource(consumer).map(new Tokenizer()).keyBy(0).sum(1).print()；
        env.execute("Streaming WordCount");


       // DataStream<String> stream = env.addSource(consumer);
        //DataStream<Tuple2<String, Integer>> counts = stream.flatMap(new LineSplitter()).keyBy(0).sum(1);

        //counts.print();
    }
}


class Tokenizer implements FlatMapFunction<String, Tuple2<String, Integer>> {

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
        }

    }


