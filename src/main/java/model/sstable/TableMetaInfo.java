package model.sstable;

import java.io.RandomAccessFile;

/**
 * @Author taoxier
 * @Date 2024/7/7 22:24
 * @注释 ssTable索引信息
 */
public class TableMetaInfo {

    /**
     * 版本号
     */
    private long version;

    /**
     * 数据区开始
     */
    private long dataStart;

    /**
     * 数据区长度
     */
    private long dataLen;

    /**
     * 稀疏索引区开始
     */
    private long indexStart;

    /**
     * 稀疏索引区长度
     */
    private long indexLen;

    /**
     * 分段大小阈值
     */
    private long partSize;

    /**
     * @描述 写文件
     * @param file
     * @return void
     * @Author taoxier
     */
    public void writeToFile(RandomAccessFile file) {
        try {
            file.writeLong(partSize);
            file.writeLong(dataStart);
            file.writeLong(dataLen);
            file.writeLong(indexStart);
            file.writeLong(indexLen);
            file.writeLong(version);
        } catch (Throwable t) {
            throw new RuntimeException(t);
        }
    }

    /**
     * @描述 读文件
     * @param file
     * @return TableMetaInfo
     * @Author taoxier
     */
    public static TableMetaInfo readFromFile(RandomAccessFile file) {
        try {
            TableMetaInfo tmi = new TableMetaInfo();
            long fileLen = file.length();

            file.seek(fileLen - 8);
            tmi.setVersion(file.readLong());

            file.seek(fileLen - 8 * 2);
            tmi.setIndexLen(file.readLong());

            file.seek(fileLen - 8 * 3);
            tmi.setIndexStart(file.readLong());

            file.seek(fileLen - 8 * 4);
            tmi.setDataLen(file.readLong());

            file.seek(fileLen - 8 * 5);
            tmi.setDataStart(file.readLong());

            file.seek(fileLen - 8 * 6);
            tmi.setPartSize(file.readLong());

            return tmi;
        } catch (Throwable t) {
            throw new RuntimeException(t);
        }

    }

    public long getVersion() {
        return version;
    }

    public void setVersion(long version) {
        this.version = version;
    }

    public long getDataStart() {
        return dataStart;
    }

    public void setDataStart(long dataStart) {
        this.dataStart = dataStart;
    }

    public long getDataLen() {
        return dataLen;
    }

    public void setDataLen(long dataLen) {
        this.dataLen = dataLen;
    }

    public long getIndexStart() {
        return indexStart;
    }

    public void setIndexStart(long indexStart) {
        this.indexStart = indexStart;
    }

    public long getIndexLen() {
        return indexLen;
    }

    public void setIndexLen(long indexLen) {
        this.indexLen = indexLen;
    }

    public long getPartSize() {
        return partSize;
    }

    public void setPartSize(long partSize) {
        this.partSize = partSize;
    }
}
