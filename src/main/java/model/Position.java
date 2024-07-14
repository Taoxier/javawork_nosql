package model;

/**
 * @Author taoxier
 * @Date 2024/7/7 22:26
 * @注释 数据段位置信息
 */
public class Position {
    //开始位置
    private long start;

    //长度
    private long len;

    public Position(long start, long len) {
        this.start = start;
        this.len = len;
    }

    public long getStart() {
        return start;
    }

    public void setStart(long start) {
        this.start = start;
    }

    public long getLen() {
        return len;
    }

    public void setLen(long len) {
        this.len = len;
    }
}
