package com.hillstone;

import com.alibaba.fastjson.JSON;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.typeutils.TypeExtractor;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.AssignerWithPeriodicWatermarks;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.streaming.connectors.kafka.KafkaTableSource;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.apache.flink.table.sources.TableSource;
import org.apache.flink.types.Row;

import javax.annotation.Nullable;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Properties;

/**
 * @author: ljyang
 * @date: 2019/7/1 20:11
 * @description
 */
public class StreamingJob2 {
    public static void main(String[] args) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment tableEnv = TableEnvironment.getTableEnvironment(env);

        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", "10.180.15.68:9092");
        properties.setProperty("group.id", "demo-flink");
        properties.setProperty("enable.auto.commit", "true");



        /*
        TableSource tableSource = new KafkaTableSource(new TableSchema(new String[]{"word", "count"},
                new TypeInformation[]{Types.STRING, Types.INT}), "demo-flink", properties, new TableDeserialization());

        tableEnv.registerTableSource("wordCount", tableSource);

        Table res = tableEnv.scan("wordCount").groupBy(Row.).select("count.sum");
        tableEnv.toRetractStream(res, Row.class).print();
        */

        /*
        DataStream<WordCount> dataStream = env.addSource(new FlinkKafkaConsumer<>("demo-flink", new SimpleStringSchema(), properties))
                .map((String str) -> JSON.parseObject(str, WordCount.class));
        Table table = tableEnv.fromDataStream(dataStream);
        tableEnv.registerTable("table1", table);

        tableEnv.toRetractStream(table, WordCount.class).print();

        Table resultTable = tableEnv.scan("table1").groupBy("word").select("word, count.sum AS count");
        tableEnv.toRetractStream(resultTable, WordCount.class).print();
        */

        try {
            env.execute();
        } catch (Exception e) {
            e.printStackTrace();
        }

    }
    public static class WordCount {
        private String word;
        private int count;

        public WordCount() {
        }

        public String getWord() {
            return word;
        }

        public void setWord(String word) {
            this.word = word;
        }

        public int getCount() {
            return count;
        }

        public void setCount(int count) {
            this.count = count;
        }

        @Override
        public String toString() {
            return "WordCount{" +
                    "word='" + word + '\'' +
                    ", count=" + count +
                    '}';
        }
    }
}

//{"word": "a", "count": 10}

class TableDeserialization implements DeserializationSchema<Row>{

    @Override
    public Row deserialize(byte[] bytes) throws IOException {
        String str = new String(bytes, StandardCharsets.UTF_8);
        if("".equals(str)){
            return null;
        }
        StreamingJob2.WordCount wc = JSON.parseObject(str, StreamingJob2.WordCount.class);
        return Row.of(wc.getWord(), wc.getCount());
    }

    @Override
    public boolean isEndOfStream(Row row) {
        return false;
    }

    @Override
    public TypeInformation<Row> getProducedType() {
        return TypeExtractor.getForClass(Row.class);
    }
}

class M implements AssignerWithPeriodicWatermarks {

    @Nullable
    @Override
    public Watermark getCurrentWatermark() {
        return null;
    }

    @Override
    public long extractTimestamp(Object o, long l) {
        return 0;
    }
}