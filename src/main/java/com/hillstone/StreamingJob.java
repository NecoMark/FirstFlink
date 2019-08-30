/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.hillstone;

import com.esotericsoftware.minlog.Log;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.SlidingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.slf4j.LoggerFactory;
import org.slf4j.Logger;
import com.alibaba.fastjson.*;

import java.util.Properties;

public class StreamingJob {
    public static Logger LOG = LoggerFactory.getLogger(StreamingJob.class);

    public static void main(String[] args) throws Exception {
        // set up the streaming execution environment

//        SlidingProcessingTimeWindows
        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", "10.180.15.68:9092");
        properties.setProperty("group.id", "demo-flink");
        properties.setProperty("enable.auto.commit", "true");

        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        DataStream<String> sourceStream = env.addSource(new FlinkKafkaConsumer<>("demo-flink", new SimpleStringSchema(), properties));

        StringBuffer sb = new StringBuffer();
        // 若在括号处需要进行换行
        sb.append("a").append("b").append(
            "c");

        // 若在函数参数处需要换行
        test("a", "b",
            "c");

        // 若在运算符处需要换行
        DataStream<Double> dataStream = sourceStream
            .map((String jsonStr) -> JSONObject.parseObject(jsonStr))
            .keyBy((JSONObject jsObject) -> jsObject.getString("key"))
            .timeWindow(Time.seconds(2))
            .aggregate(new WindowAggregateFunction());
        dataStream.print();
        env.execute("Flink Streaming Java API Skeleton");
    }
    public static void test(String ...args){}

}

class WindowAggregateFunction implements AggregateFunction<JSONObject, DemoAccumulator, Double> {

    @Override
    public DemoAccumulator createAccumulator() {
        return new DemoAccumulator();
    }

    @Override
    public DemoAccumulator add(JSONObject jsonObject, DemoAccumulator demoAccumulator) {
        demoAccumulator.setSum(jsonObject.getIntValue("value") + demoAccumulator.getSum());
        demoAccumulator.setAve(demoAccumulator.getAve() + 1);
        return demoAccumulator;
    }

    @Override
    public Double getResult(DemoAccumulator demoAccumulator) {
        Log.info("++++++++++++++++++++++++" + demoAccumulator.getSum());
        return (double) demoAccumulator.getSum() / demoAccumulator.getAve();
    }

    @Override
    public DemoAccumulator merge(DemoAccumulator demoAccumulator, DemoAccumulator acc1) {
        demoAccumulator.setSum(demoAccumulator.getSum() + acc1.getSum());
        demoAccumulator.setAve(demoAccumulator.getAve() + acc1.getAve());
        return demoAccumulator;
    }
}

