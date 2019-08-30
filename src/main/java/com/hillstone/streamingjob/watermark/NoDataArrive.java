package com.hillstone.streamingjob.watermark;

import org.apache.flink.streaming.api.watermark.Watermark;

/**
 * @author: ljyang
 * @date: 2019/7/11 15:31
 * @description
 */
public class NoDataArrive implements WatermarkStrategy {
    @Override
    public Watermark makeWatermark() {
        return new Watermark(1);
    }
}
