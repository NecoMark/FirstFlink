package com.hillstone;

import com.hillstone.streamingjob.watermark.WatermarkStrategy;
import org.apache.flink.streaming.api.watermark.Watermark;

/**
 * @author: ljyang
 * @date: 2019/7/11 15:33
 * @description
 */
public class Context {
    private WatermarkStrategy watermarkStrategy;

    public Context(WatermarkStrategy watermarkStrategy) {
        this.watermarkStrategy = watermarkStrategy;
    }

    public Watermark makeWatermark(){
        return watermarkStrategy.makeWatermark();
    }

}
