package com.hillstone;

public class DemoAccumulator {
    public DemoAccumulator(int sum, int ave) {
        this.sum = sum;
        this.ave = ave;
    }

    public int getSum() {
        return sum;
    }

    public void setSum(int sum) {
        this.sum = sum;
    }

    public int getAve() {
        return ave;
    }

    public void setAve(int ave) {
        this.ave = ave;
    }

    private int sum;
    private int ave;

    public DemoAccumulator() {
        sum = 0;
        ave = 0;
    }
}
