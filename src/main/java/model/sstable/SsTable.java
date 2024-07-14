package model.sstable;

import com.alibaba.fastjson.JSONObject;
import com.alibaba.fastjson.TypeReference;
import jdk.javadoc.internal.doclets.formats.html.NestedClassWriterImpl;
import model.Position;
import model.command.Command;
import model.command.RmCommand;
import model.command.SetCommand;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import utils.CommandUtil;
import utils.LoggerUtil;

import java.io.Closeable;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.charset.StandardCharsets;
import java.util.LinkedList;
import java.util.Optional;
import java.util.TreeMap;
import java.util.function.Consumer;


/**
 * @Author taoxier
 * @Date 2024/7/7 22:24
 * @注释 数据表
 */
public class SsTable implements Closeable {

    private static final String RW_MODE = "rw";
    private final Logger LOGGER = LoggerFactory.getLogger(SsTable.class);

    /**
     * 稀疏索引
     */
    private TreeMap<String, Position> sparseIndex;

    /**
     * 文件索引信息
     */
    private TableMetaInfo tableMetaInfo;

    /**
     * 文件句柄
     */
    private final RandomAccessFile tableFile;

    /**
     * 文件路径
     */
    private final String filePath;

    private SsTable(String filePath, int partSize) {
        this.tableMetaInfo = new TableMetaInfo();//索引
        this.tableMetaInfo.setPartSize(partSize);
        this.filePath = filePath;
        try {
            this.tableFile = new RandomAccessFile(filePath, RW_MODE);
            tableFile.seek(0);
        } catch (Throwable t) {
            throw new RuntimeException(t);
        }
        sparseIndex = new TreeMap<>();
    }

    /**
     * @描述 把数据段写入文件
     * @param partData
     * @return void
     * @Author taoxier
     */
    private void writeDataPart(JSONObject partData) {
        try {
            byte[] partDataBytes = partData.toJSONString().getBytes(StandardCharsets.UTF_8);
            long start = tableFile.getFilePointer();//记录开始位置
            tableFile.write(partDataBytes);//写

            //记录数据段的第一个key到稀疏索引中
            Optional<String> firstKey = partData.keySet().stream().findFirst();//取第一个key
//            firstKey.ifPresent(s -> sparseIndex.put(s, new Position(start, partDataBytes.length)));//存
            //存
            firstKey.ifPresent(new Consumer<String>() {
                @Override
                public void accept(String s) {
                    sparseIndex.put(s, new Position(start, partDataBytes.length));
                }
            });
            partData.clear();//写完清空
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * @描述 根据内存表创建数据表ssTable
     * @param memTable
     * @return void
     * @Author taoxier
     */
    private void initFromMemTable(TreeMap<String, Command> memTable) {
        try {
            JSONObject partData = new JSONObject(true);//保持属性的插入顺序
            tableMetaInfo.setDataStart(tableFile.getFilePointer());//获取当前流在文件中的读/写位置(文件指针)
            for (Command cmd : memTable.values()) {
                //set
                if (cmd instanceof SetCommand) {
                    SetCommand set = (SetCommand) cmd;
                    partData.put(set.getKey(), set);
                }

                //rm
                if (cmd instanceof SetCommand) {
                    RmCommand rm = (RmCommand) cmd;
                    partData.put(rm.getKey(), rm);
                }

                //如果达到分段阈值，写入数据段
                if (partData.size() >= tableMetaInfo.getPartSize()) {
                    writeDataPart(partData);
                }
            }

            //最后可能剩点
            if (partData.size() > 0) {
                writeDataPart(partData);
            }

            long dataPartLen = tableFile.getFilePointer();//获取当前文件指针
            tableMetaInfo.setDataLen(dataPartLen);//记录数据区长度

            //保存稀疏索引
            byte[] indexBytes = JSONObject.toJSONString(sparseIndex).getBytes(StandardCharsets.UTF_8);
            tableMetaInfo.setIndexStart(tableFile.getFilePointer());//记录稀疏索引开始位置
            tableFile.write(indexBytes);//写入稀疏索引
            tableMetaInfo.setIndexLen(indexBytes.length);//记录稀疏索引区长度
            LoggerUtil.debug(LOGGER, "[SsTable][initFromIndex][sparseIndex]: {}", sparseIndex);

            //保存文件索引信息
            tableMetaInfo.writeToFile(tableFile);
            LoggerUtil.info(LOGGER, "[SsTable][initFromIndex]: {},{}", filePath, tableMetaInfo);

        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * @描述 根据文件创建ssTable
     * @param
     * @return void
     * @Author taoxier
     */
    private void initFromFile() {
        try {
            //先读取索引内容区
            TableMetaInfo tableMetaInfo = TableMetaInfo.readFromFile(tableFile);
            LoggerUtil.debug(LOGGER, "[SsTable][initFromFile][tableMetaInfo]: {}", tableMetaInfo);

            //再读稀疏索引区
            byte[] indexBytes = new byte[(int) tableMetaInfo.getIndexLen()];
            tableFile.seek(tableMetaInfo.getDataStart());//跳到稀疏索引区开始位置
            tableFile.read(indexBytes);//读稀疏索引区
            String indexString = new String(indexBytes, StandardCharsets.UTF_8);
            LoggerUtil.debug(LOGGER, "[SsTable][initFromFile][indexStr]: {}", indexString);
            sparseIndex = JSONObject.parseObject(indexString, new TypeReference<TreeMap<String, Position>>() {
            });//存到稀疏索引
            this.tableMetaInfo = tableMetaInfo;//记录文件索引信息
            LoggerUtil.debug(LOGGER, "[SsTable][initFromFile][sparseIndex]: {}", sparseIndex);

        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * @描述 根据内存表创建ssTable
     * @param filePath
     * @param partSize
     * @param memTable
     * @return SsTable
     * @Author taoxier
     */
    public static SsTable createFromMemTable(String filePath, int partSize, TreeMap<String, Command> memTable) {
        SsTable ssTable = new SsTable(filePath, partSize);
        ssTable.initFromMemTable(memTable);
        return ssTable;
    }

    /**
     * @描述 根据文件创建ssTable，即把文件内容恢复到ssTable
     * @param filePath
     * @return SsTable
     * @Author taoxier
     */
    public static SsTable createFromFile(String filePath) {
        SsTable ssTable = new SsTable(filePath, 0);
        ssTable.initFromFile();
        return ssTable;
    }

    /**
     * @描述 从ssTable中查询数据
     * @param key
     * @return Command
     * @Author taoxier
     */
    public Command query(String key) {
        try {
            LinkedList<Position> sparseKeyPositionSection = new LinkedList<>();
            Position lastSmallPosition = null;
            Position firstBigPosition = null;

            //遍历稀疏索引，找到key所在的区间
            for (String k : sparseIndex.keySet()) {
                if (k.compareTo(key) <= 0) {
                    lastSmallPosition = sparseIndex.get(k);
                } else {
                    firstBigPosition = sparseIndex.get(k);
                    break;
                }
            }
            //如果存在最后一个小于key的稀疏索引位置
            if (lastSmallPosition != null) {
                sparseKeyPositionSection.add(lastSmallPosition);
            }
            //如果存在第一个大于key的稀疏索引位置
            if (firstBigPosition != null) {
                sparseKeyPositionSection.add(firstBigPosition);
            }
            //如果都不存在，即找不到key所在的区间，key不存在
            if (sparseKeyPositionSection.size() == 0) {
                return null;
            }
            LoggerUtil.debug(LOGGER, "[SsTable][initFromFile][sparseKeyPositionSection]: {}", sparseKeyPositionSection);

            //开始在区间中寻找key
            Position firstKeyPosition = sparseKeyPositionSection.getFirst();//区间第一个位置
            Position lastKeyPosition = sparseKeyPositionSection.getLast();//区间最后一个位置
            long start = 0;
            long len = 0;
            start = firstKeyPosition.getStart();//开始的key
            if (firstKeyPosition.equals(lastKeyPosition)) {
                //如果区间开始位置和区间结束位置相同，即该key在最开头或最后面
                len = firstKeyPosition.getLen();
            } else {
                len = lastKeyPosition.getStart() + lastKeyPosition.getLen() - start;
            }

            //在区间寻找key
            byte[] dataPart = new byte[(int) len];
            tableFile.seek(start);
            tableFile.read(dataPart);//获取区间的数据
            int dataPartStart = 0;

            for (Position position : sparseKeyPositionSection) {
                JSONObject dataPartJson = JSONObject.parseObject(new String(dataPart, dataPartStart, (int) position.getLen()));//指定的起始位置和长度
                LoggerUtil.debug(LOGGER, "[SsTable][initFromFile][dataPartJson]: {}", dataPartJson);
                if (dataPartJson.containsKey(key)) {
                    JSONObject value = dataPartJson.getJSONObject(key);
                    return CommandUtil.jsonToCommand(value);//将value对象转换为特定的类型，并返回该对象
                }
                dataPartStart += (int) position.getLen();//更新dataPartStart的值，移动到下一个Position的起始位置
            }
            return null;
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void close() throws IOException {

    }
}
