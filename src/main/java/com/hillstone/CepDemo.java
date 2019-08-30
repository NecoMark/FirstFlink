package com.hillstone;

import com.alibaba.fastjson.JSONObject;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.cep.CEP;
import org.apache.flink.cep.PatternFlatSelectFunction;
import org.apache.flink.cep.PatternStream;
import org.apache.flink.cep.pattern.Pattern;
import org.apache.flink.cep.pattern.conditions.SimpleCondition;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.util.Collector;

import java.util.List;
import java.util.Map;
import java.util.Properties;

/**
 * @author: ljyang
 * @date: 2019/7/15 9:28
 * @description
 */
public class CepDemo {
    public static void main(String[] args) {

        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", "10.180.15.68:9092");
        properties.setProperty("group.id", "demo-flink");
        properties.setProperty("enable.auto.commit", "true");

        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        DataStream<Event> sourceStream = env.addSource(new FlinkKafkaConsumer<>("demo-cep", new SimpleStringSchema(), properties))
                .map(str -> JSONObject.parseObject(str, Event.class));

        Pattern<Event, ?> pattern1 = Pattern.<Event>begin("start").where(new SimpleCondition<Event>() {
            @Override
            public boolean filter(Event event) throws Exception {
                return event.getEventId() == 1;
            }
        }).next("middle").where(new SimpleCondition<Event>() {
            @Override
            public boolean filter(Event event) throws Exception {
                return event.getType() == 1;
            }
        }).followedBy("end").where(new SimpleCondition<Event>() {
            @Override
            public boolean filter(Event event) throws Exception {
                return "a".equals(event.getName());
            }
        });

        PatternStream<Event> patternStream = CEP.pattern(sourceStream, pattern1);
        DataStream<String> dataStream = patternStream.flatSelect(new PatternFlatSelectFunction<Event, String>() {
            @Override
            public void flatSelect(Map<String, List<Event>> map, Collector<String> collector) throws Exception {
                collector.collect(JSONObject.toJSONString(map.get("start")));
            }
        });
        dataStream.print();

    }
}

class Event{
    private int eventId;
    private int type;
    private String name;


    public Event() {
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public int getEventId() {
        return eventId;
    }

    public int getType() {
        return type;
    }

    public void setType(int type) {
        this.type = type;
    }

    public void setEventId(int eventId) {
        this.eventId = eventId;
    }
}