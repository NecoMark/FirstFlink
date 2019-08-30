package com.hillstone.streamingjob.watermark;

import org.apache.flink.streaming.api.watermark.Watermark;

/**
 * author: ljyang
 *
 * @date: 2019/7/11 15:28
 * @description
 */
public interface WatermarkStrategy {
    Watermark makeWatermark();
}
