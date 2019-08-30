package com.hillstone.streamingjob.watermark;

import com.alibaba.fastjson.JSONObject;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.AssignerWithPeriodicWatermarks;
import org.apache.flink.streaming.api.functions.AssignerWithPunctuatedWatermarks;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;

import javax.annotation.Nullable;
import java.util.Properties;

/**
 * @author: ljyang
 * @date: 2019/7/11 15:24
 * @description
 */
public class WatermarkDemo1 {
    public static void main(String[] args) {
        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", "10.180.15.68:9092");
        properties.setProperty("group.id", "demo-flink");
        properties.setProperty("enable.auto.commit", "true");

        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        env.getConfig().setAutoWatermarkInterval(1000);
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        DataStream<String> sourceStream = env.addSource(new FlinkKafkaConsumer<>("demo-flink", new SimpleStringSchema(), properties));
        DataStream<String> data = sourceStream
                .filter(str -> {return str == ""?false:true;})
                .map(str -> JSONObject.parseObject(str))
                .assignTimestampsAndWatermarks(new MyTimestampsAndPeriodWatermarks(5000, 1000))
                .keyBy(jsonObj -> jsonObj.getString("key"))
                .window(TumblingEventTimeWindows.of(Time.seconds(5)))
                .reduce((value1, value2) ->  {value1.put("count", value1.getIntValue("count") + value2.getIntValue("count")); return value1;})
                .map(new MapFunction<JSONObject, String>() {
                    @Override
                    public String map(JSONObject jsonObject) throws Exception {
                        return JSONObject.toJSONString(jsonObject);
                    }
                });
        data.print();
        try {
            env.execute("w");
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}

//第一种情况：长时间没有新数据到达，需要手动调整水印使窗口触发聚合计算。
//第二种情况：有新数据到达，但是新数据时间戳没有增大，导致watermark没有变化，是否需要手动调整水印？
//第三种情况: 有新数据到达，新数据时间戳也在增大，但是增长速度很慢，是否需要手动调整水印？

class MyTimestampsAndPeriodWatermarks implements AssignerWithPeriodicWatermarks<JSONObject> {

    //周期性生成水印
    private long maxTimeLag = 1000L;
    private long windowSize = 0L;

    private long currentMaxTimestamp = 0L;
    //只有新数据到达，且时间戳更大 currentMaxTimestamp才会更新
    private long windowPurgeTime = 0L;
    private long lastDataArriveTime = 0L;

    public MyTimestampsAndPeriodWatermarks(long windowSize, long maxTimeLag) {
        this.windowSize = windowSize;
        this.maxTimeLag = maxTimeLag;
    }

    @Nullable
    @Override
    public Watermark getCurrentWatermark() {
        //水印会定期生成，但不一定有数据到达。
        long diff = windowPurgeTime - currentMaxTimestamp;
        long curTime = System.currentTimeMillis();

        if (diff > 0 && curTime - lastDataArriveTime > diff){
            //若长时间没有数据到达，手动将水印调整，使窗口触发聚合计算
            currentMaxTimestamp = windowPurgeTime;
            lastDataArriveTime = curTime;
        }

        Watermark watermark = new Watermark(currentMaxTimestamp - maxTimeLag);
        System.out.println(watermark.toString());
        return watermark;
    }

    @Override
    public long extractTimestamp(JSONObject jsonObject, long lastTimestamp) {
        long timestamp = jsonObject.getLong("timestamp");
        if (timestamp > currentMaxTimestamp){
            currentMaxTimestamp = timestamp;
        }
        lastDataArriveTime = System.currentTimeMillis();
        windowPurgeTime = Math.max(windowPurgeTime, getWindowExpireTime(currentMaxTimestamp, 0));

        //了防止长时间没有数据到达，而导致窗口不能闭合
        //假设将currentMaxTimestamp作为起始时间，求得的一个闭合时间在 当前窗口下一个窗口的中间部分，
        //这个时间能够保证当前窗口触发计算，而且不影响下一个窗口的计算。
        return timestamp;
    }

    private long getWindowExpireTime(long timestamp, long offset){
        //得到窗口执行聚合的时间
        long startTime = TimeWindow.getWindowStartWithOffset(timestamp, offset, windowSize);
        long endTime = startTime + windowSize;
        long purgeTime = endTime + maxTimeLag;
        return purgeTime;
    }

}


class NoNewWatermark implements AssignerWithPeriodicWatermarks<JSONObject>{
    private long maxTimeLag = 1000L;
    private long windowSize = 0L;

    private long currentMaxTimestamp = 0L;
    //只有新数据到达，且时间戳更大 currentMaxTimestamp才会更新
    private long windowPurgeTime = 0L;
    private long lastWatermarkChangeTime = 0L;

    public NoNewWatermark(long windowSize, long maxTimeLag) {
        this.maxTimeLag = maxTimeLag;
        this.windowSize = windowSize;
    }

    @Nullable
    @Override
    public Watermark getCurrentWatermark() {
        long diff = windowPurgeTime - currentMaxTimestamp;
        long curTime = System.currentTimeMillis();
        //长时间没有时间戳更大的数据到达
        if (diff > 0 && curTime - lastWatermarkChangeTime > diff){
            currentMaxTimestamp = windowPurgeTime;
            lastWatermarkChangeTime = curTime;
        }
        Watermark watermark = new Watermark(currentMaxTimestamp - maxTimeLag);
        System.out.println(watermark.toString());
        return watermark;
    }

    @Override
    public long extractTimestamp(JSONObject jsonObject, long l) {
        long timestamp = jsonObject.getLong("timestamp");
        if (timestamp > currentMaxTimestamp){
            currentMaxTimestamp = timestamp;
            lastWatermarkChangeTime = System.currentTimeMillis();
        }
        windowPurgeTime = Math.max(windowPurgeTime, getWindowExpireTime(currentMaxTimestamp, 0));
        return timestamp;
    }

    private long getWindowExpireTime(long timestamp, long offset){
        //得到窗口执行聚合的时间
        long startTime = TimeWindow.getWindowStartWithOffset(timestamp, offset, windowSize);
        long endTime = startTime + windowSize;
        long purgeTime = endTime + maxTimeLag;
        return purgeTime;
    }
}

class PunctuatedAssigner implements AssignerWithPunctuatedWatermarks<JSONObject> {
    //按条件判断是否生成水印
    //基于事件向流里注入一个WATERMARK，每一个元素都有机会判断是否生成一个WATERMARK.
    //如果得到的WATERMARK 不为空并且比之前的大就注入流中 (emitWatermark)

    @Nullable
    @Override
    public Watermark checkAndGetNextWatermark(JSONObject jsonObject, long extractedTimestamp) {
        if (true){
            return new Watermark(extractedTimestamp - 1000);
        }
        return null;
    }

    @Override
    public long extractTimestamp(JSONObject jsonObject, long l) {
        return jsonObject.getLong("timestamp");
    }
}