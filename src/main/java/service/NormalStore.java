/*
 *@Type NormalStore.java
 * @Desc
 * @Author urmsone urmsone@163.com
 * @date 2024/6/13 02:07
 * @version
 */
package service;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.alibaba.fastjson.TypeReference;
import controller.SocketServerHandler;
import model.Position;
import model.command.Command;
import model.command.CommandPos;
import model.command.RmCommand;
import model.command.SetCommand;
import model.sstable.SsTable;
import model.sstable.TableMetaInfo;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import utils.CommandUtil;
import utils.LoggerUtil;
import utils.RandomAccessFileUtil;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.channels.FileChannel;
import java.nio.channels.FileLock;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardCopyOption;
import java.util.*;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.jar.JarEntry;

public class NormalStore implements Store {

    public static final String TABLE = ".table";
    public static final String WAL = "wal";
    public static final String WAL_TMP = "walTmp";
    public static final String RW_MODE = "rw";
    public static final String NAME = "data";
    private final Logger LOGGER = LoggerFactory.getLogger(NormalStore.class);
    private final String logFormat = "[NormalStore][{}]: {}";

    /*
        1.内存，三个区，稀疏索引，查的时候按稀疏索引，看tinykvstore
        2.多线程压缩，看那个3.log那个思路
     */

    /**
     * 内存表，类似缓存
     */
    private TreeMap<String, Command> memTable;

    /**
     * 不可变内存表，用于持久化内存表中时暂存数据
     */
    private TreeMap<String, Command> immutableMemTable;

    /**
     * ssTable列表
     */
    private LinkedList<SsTable> ssTables;

    /**
     * ssTable暂存列表，用于压缩时
     */
    private LinkedList<SsTable> immutableSsTables;

    /**
     * 数据目录
     */
    private final String dataDir;

    /**
     * 读写锁，支持多线程，并发安全写入
     */
    private final ReadWriteLock indexLock;

    /**
     * 暂存数据的日志句柄
     */
    private RandomAccessFile wal;

    /**
     * 暂存数据的日志文件
     */
    private File walFile;

    /**
     * 数据分区大小
     */
    private final int partSize;

    /**
     * 持久化阈值
     */
    private final int storeThreshold;

    /**
     * 日志压缩大小阈值
     */
    private final int compressionThreshold;


    /**
     * @描述 构造方法
     * @param dataDir
     * @param storeThreshold
     * @param partSize
     * @return null
     * @Author taoxier
     */
    public NormalStore(String dataDir, int storeThreshold, int partSize, int compressionThreshold) {
        try {
            this.dataDir = dataDir;
            this.storeThreshold = storeThreshold;
            this.partSize = partSize;
            this.compressionThreshold = compressionThreshold;
            this.indexLock = new ReentrantReadWriteLock();

            File dir = new File(dataDir);
            //数据目录不存在则创建
            if (!dir.exists()) {
                LoggerUtil.info(LOGGER, logFormat, "createDataDir", "dataDir isn't exist,creating...");
                dir.mkdirs();
            }
            File[] files = dir.listFiles();
            //目录为空则不用加载ssTable
            if (files == null || files.length == 0) {
                walFile = new File(dataDir + WAL);
                wal = new RandomAccessFile(walFile, RW_MODE);
                return;
            }

            this.ssTables = new LinkedList<>();
            this.memTable = new TreeMap<>();

//            this.index = new HashMap<>();
//            this.reloadIndex();

            //从大到小加载ssTable
            TreeMap<Long, SsTable> ssTableTreeMap = new TreeMap<>(Comparator.reverseOrder());//对Long类型的键进行降序排序
            for (File file : files) {
                String fileName = file.getName();
                //如果在持久化ssTable中出现异常，则会留下WAL_TMP，需要从中恢复数据
                if (file.isFile() && fileName.equals(WAL_TMP)) {
                    restoreFromWal(new RandomAccessFile(file, RW_MODE));
                }

                //加载ssTable
                if (file.isFile() && fileName.endsWith(TABLE)) {
                    //如果是文件，并且是数据文件的话
                    int dotIndex = fileName.indexOf(".");//找到文件名中第一个点.的位置，返回点的索引
                    Long time = Long.parseLong(fileName.substring(0, dotIndex));//从文件名中提取出时间戳部分，即从文件名的开始到第一个点之间的字符，然后将其解析为一个 Long 类型的数字
                    ssTableTreeMap.put(time, SsTable.createFromFile(file.getAbsolutePath()));//放入该文件存的SsTable
                } else if (file.isFile() && fileName.endsWith(WAL)) {
                    //如果是wal文件，则加载wal
                    walFile = file;
                    wal = new RandomAccessFile(file, RW_MODE);
                    restoreFromWal(wal);
                }
            }
            ssTables.addAll(ssTableTreeMap.values());//把所有表存进SsTable
            LoggerUtil.debug(LOGGER, logFormat, "createFromFile" + ssTables);

        } catch (FileNotFoundException e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * @描述 从暂存日志wal中恢复数据放入内存表
     * @param wal
     * @return void
     * @Author taoxier
     */
    private void restoreFromWal(RandomAccessFile wal) {
        try {
            long len = wal.length();
            long start = 0;//开始位置
            wal.seek(start);//把文件指针跳到开始位置
            while (start < len) {
                //先读数据大小
                int valueLen = wal.readInt();//四个字节
                //然后根据数据大小来读数据
                byte[] bytes = new byte[valueLen];
                wal.read(bytes);//读
                JSONObject value = JSON.parseObject(new String((bytes), StandardCharsets.UTF_8));
                Command command = CommandUtil.jsonToCommand(value);//将JSON对象转换为command
                if (command != null) {
                    //如果转换成功，则把数据放进内存表
                    memTable.put(command.getKey(), command);
                }
                start += 4;
                start += valueLen;//跳过数据长度
            }
            wal.seek(wal.length());//跳到文件末尾
        } catch (Throwable t) {
            throw new RuntimeException(t);
        }
    }

    //----------------------
    public String genFilePath() {
        return this.dataDir + File.separator + NAME + TABLE;
    }

    public String genWalPath() {
        return this.dataDir + File.separator + WAL + TABLE;
    }

    public String getNextLogFileGenName() {
        return this.dataDir + File.separator + NAME + "_compress" + TABLE;
    }

    //---------------------------------


    //重新加载索引
//    public void reloadIndex() {
//        try {
//            RandomAccessFile file = new RandomAccessFile(this.genFilePath(), RW_MODE);
//            long len = file.length();
//            long start = 0;
//            file.seek(start);
//            while (start < len) {
//                int cmdLen = file.readInt();
//                byte[] bytes = new byte[cmdLen];
//                file.read(bytes);
//                JSONObject value = JSON.parseObject(new String(bytes, StandardCharsets.UTF_8));
//                Command command = CommandUtil.jsonToCommand(value);
//                start += 4;
//                if (command != null) {
//                    CommandPos cmdPos = new CommandPos((int) start, cmdLen);
//                    index.put(command.getKey(), cmdPos);
//                }
//                start += cmdLen;
//            }
//            file.seek(file.length());
//            file.close();
//        } catch (Exception e) {
//            e.printStackTrace();
//        }
//        LoggerUtil.debug(LOGGER, logFormat, "reloadIndex",index.toString());
//    }

    //去重操作
//    private TreeMap<String, Command> deduplicateCommands() {
//        TreeMap<String, Command> latestCommands = new TreeMap<>(); // 用于存储每个key的最新命令
//        try (RandomAccessFile raf = new RandomAccessFile(this.genFilePath(), RW_MODE)) {
//            long length = raf.length();
//            long start = 0;
//            while (start < length) {
//                raf.seek(start);
//                int cmdLen = raf.readInt();
//                byte[] cmdBytes = new byte[cmdLen];
//                raf.read(cmdBytes);
//                Command command = CommandUtil.jsonToCommand(JSONObject.parseObject(new String(cmdBytes, StandardCharsets.UTF_8)));
//                if (command != null) {
//                    // 只保留每个key的最新命令
//                    latestCommands.put(command.getKey(), command);
//                }
//                start += 4 + cmdLen; // 移动到下一个命令
//            }
//        } catch (IOException e) {
//            throw new RuntimeException("Error during deduplication process", e);
//        }
////        LoggerUtil.debug(LOGGER,logFormat,"Deduplication");
//        return latestCommands;
//    }

    //实现压缩
//    private void compressLogFile() {
//        TreeMap<String, Command> latestCommands = deduplicateCommands();
//        String newLogFile = getNextLogFileGenName(); // 生成新的日志文件名
//        String tempLogFile = newLogFile + ".tmp"; // 新增临时文件名用于中间步骤
//
//        try (RandomAccessFile newRaf = new RandomAccessFile(tempLogFile, RW_MODE)) {
//            for (Map.Entry<String, Command> entry : latestCommands.entrySet()) {
//                Command command = entry.getValue();
//                byte[] cmdBytes = JSONObject.toJSONBytes(command);
//                RandomAccessFileUtil.writeInt(tempLogFile, cmdBytes.length);
//                RandomAccessFileUtil.write(tempLogFile, cmdBytes);
//            }
//        } catch (IOException e) {
//            throw new RuntimeException("Error writing to the temporary compressed log file", e);
//        }
//
//        // 使用Files.move进行原子性重命名操作
//        try {
//            Files.move(Paths.get(tempLogFile), Paths.get(newLogFile), StandardCopyOption.REPLACE_EXISTING, StandardCopyOption.ATOMIC_MOVE);
//        } catch (IOException e) {
//            throw new RuntimeException("Failed to atomically rename temporary file to new log file", e);
//        }
//
//        // 替换原日志文件
//        // 使用Files.move进行原子性替换原文件
//        try {
////            Files.move(Paths.get(newLogFile), Paths.get(originalFile.getAbsolutePath()), StandardCopyOption.REPLACE_EXISTING, StandardCopyOption.ATOMIC_MOVE);
//            Files.move(Paths.get(newLogFile), Paths.get(this.genFilePath()), StandardCopyOption.REPLACE_EXISTING, StandardCopyOption.ATOMIC_MOVE);
//        } catch (IOException e) {
//            throw new RuntimeException("Failed to atomically replace original log file with compressed one", e);
//        }
//
//        // 重新加载索引，以适应新的日志文件结构
//        reloadIndex();
//    }

    /**
     * @描述 切换内存表  在持久化内存表时，新建一个用，存旧的内存表
     * @param
     * @return void
     * @Author taoxier
     */
    private void switchMemTable() {
        try {
            indexLock.writeLock().lock();
            //切换内存表
            immutableMemTable = memTable;//不可变内存表，暂存数据
            memTable = new TreeMap<>();
            //切换内存表的同时也切换wal
            wal.close();
            File tmpWal = new File(dataDir + WAL_TMP);
            if (tmpWal.exists()) {
                if (!tmpWal.delete()) {
                    //如果存在暂存wal文件，尝试删除，如果不能删则抛出异常
                    throw new RuntimeException("-[异常抛出]：删除 'tmpWal' 失败");
                }
            }
            if (!walFile.renameTo(tmpWal)) {
                throw new RuntimeException("-[异常抛出]：重命名 'walFile' 变为 'tmpWal' 失败");
            }
            walFile = new File(dataDir + WAL);
            wal = new RandomAccessFile(walFile, RW_MODE);
        } catch (FileNotFoundException e) {
            throw new RuntimeException(e);
        } catch (IOException e) {
            throw new RuntimeException(e);
        } finally {
            indexLock.writeLock().unlock();
            LoggerUtil.debug(LOGGER, logFormat, "switchMemTable");
        }
    }

    /**
     * @描述 把内存表持久化到SsTable
     * @param
     * @return void
     * @Author taoxier
     */
    private void storeSsTable() {
        try {
            SsTable ssTable = SsTable.createFromMemTable(dataDir + System.currentTimeMillis() + TABLE, partSize, immutableMemTable);//按照时间命名 创内存表对应的ssTable
            ssTables.addFirst(ssTable);//插在开头

            //存完了可以重置暂存内存表和删临时Wal
            immutableMemTable = null;
            File tmpWal = new File(dataDir + WAL_TMP);
            if (tmpWal.exists()) {
                if (!tmpWal.delete()) {
                    throw new RuntimeException("-[异常抛出]：删除 'tmpWal' 失败");
                }
            }
        } catch (RuntimeException e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * @描述 压缩时切换ssTables列表
     * @param
     * @return void
     * @Author taoxier
     */
    private void switchSsTables() {
        try {
            indexLock.writeLock().lock();
            //切换ssTables列表
            immutableSsTables = ssTables;
            //清空ssTables
            ssTables = new LinkedList<>();
        } catch (Exception e) {
            throw new RuntimeException(e);
        } finally {
            indexLock.writeLock().unlock();
            LoggerUtil.debug(LOGGER, logFormat, "switchSsTables");
        }
    }

    /**
     * @描述 压缩数据表
     * 把列表里的ssTable从新到旧遍历，遇到新的数据就存到表里，遇到重复的数据就下一个
     * @param
     * @return void
     * @Author taoxier
     */
    private void compressSsTables() throws IOException {
        TreeMap<String, Command> compressTable = new TreeMap<>();//去重后存到这里，压缩表
        for (SsTable ssTable : immutableSsTables) {
            try {
                //读稀疏索引和文件句柄
                TreeMap<String, Position> sparseIndex = ssTable.getSparseIndex();
                RandomAccessFile tableFile = ssTable.getTableFile();
                //读文件元信息
                TableMetaInfo tableMetaInfo = ssTable.getTableMetaInfo();
                //遍历稀疏索引
                for (Map.Entry<String, Position> entry : sparseIndex.entrySet()) {
                    String key = entry.getKey();
                    Position position = entry.getValue();
                    long dataStart = position.getStart();
                    long dataLen = position.getLen();

                    if (!compressTable.containsKey(key)) {
                        //如果压缩表里没有该key
                        tableFile.seek(dataStart);//移动到数据开始位置
                        byte[] partDataBytes = new byte[(int) dataLen];

                        //读具体数据
                        tableFile.readFully(partDataBytes);
                        String dataString = new String(partDataBytes, StandardCharsets.UTF_8);
                        JSONObject dataObject = JSON.parseObject(dataString);
                        dataObject.keySet().forEach(keyObj -> {
                            JSONObject cmdObj = dataObject.getJSONObject(keyObj);
                            Command cmd = CommandUtil.jsonToCommand(cmdObj);
                            //插到压缩表
                            if (cmd != null) {
                                compressTable.put(key, cmd);
                            }
                        });
                    }
                }
                tableFile.close();//!!!!!!!---------问题出在这里啊啊啊啊
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }
        LoggerUtil.debug(LOGGER, logFormat, "compressSsTables");

        //把ssTables都遍历完后，去重的数据都存在压缩表compressTable里
        //删除对应的数据文件，将压缩表转化成ssTable并加到ssTables列表中
        try {
            indexLock.writeLock().lock();

            for (SsTable ssTable : immutableSsTables) {
                File file = new File(ssTable.getFilePath());
                if (file.exists()) {
                    //删除ssTable对应的数据文件
                    if (!file.delete()) {
                        throw new RuntimeException("-[异常抛出]：删除ssTable数据文件失败");
                    }

                }
            }
        } catch (Exception e) {
            e.printStackTrace(); // 打印详细的异常信息
        }
        LoggerUtil.debug(LOGGER, logFormat, "deleteSsTableFile");
        indexLock.writeLock().unlock();

        SsTable ssTable = SsTable.createFromCompressTable(dataDir + System.currentTimeMillis() + TABLE, partSize, compressTable);//按照时间命名 创内存表对应的ssTable

        ssTables.addFirst(ssTable);//插在开头
        immutableSsTables = new LinkedList<>();//清空暂存压缩数据表
        LoggerUtil.debug(LOGGER, logFormat, "addCompressTableToSsTables");
    }

    /**
     * @描述 检查是否需要压缩
     * @param
     * @return void
     * @Author taoxier
     */
    private void checkIfCompress() throws IOException {
        if (ssTables.size() > compressionThreshold) {
            //切换ssTables列表
            switchSsTables();
            //压缩
            compressSsTables();
        }
    }

    /**
     * @描述 增改
     * @param key
     * @param value
     * @return void
     * @Author taoxier
     */
    @Override
    public void set(String key, String value) {
        try {
            SetCommand command = new SetCommand(key, value);
            byte[] commandBytes = JSONObject.toJSONBytes(command);
            // 加锁
            indexLock.writeLock().lock();

            //写wal
            wal.writeInt(commandBytes.length);
            wal.write(commandBytes);
            //写内存表
            memTable.put(key, command);

            //内存表达到一定阀值，写进SsTable
            if (memTable.size() > storeThreshold) {
                //切换内存表
                switchMemTable();
                //持久化到SsTable
                storeSsTable();
            }
            //检查是否需要压缩
            checkIfCompress();

        } catch (Throwable t) {
            throw new RuntimeException(t);
        } finally {
            //解锁
            indexLock.writeLock().unlock();
        }
    }

    /**
     * @描述 获取
     * @param key
     * @return String
     * @Author taoxier
     */
    @Override
    public String get(String key) {
        try {
            indexLock.readLock().lock();
            //先从内存表中找
            Command command;
            command = memTable.get(key);
            if (command == null && immutableMemTable != null) {
                //如果找不到，可能处于持久化过程中，从暂存内存表中找
                command = immutableMemTable.get(key);
            }
            //如果还没有，那在ssTable中找，从新的找到旧
            if (command == null) {
                for (SsTable ssTable : ssTables) {
                    command = ssTable.query(key);
                    if (command != null) {
                        //找到就退出循环
                        break;
                    }
                }
            }

            if (command instanceof SetCommand) {
                //如果是set命令 返回对应的值
                return ((SetCommand) command).getValue();
            }
            if (command instanceof RmCommand) {
                //如果是rm命令 返回null
                return null;
            }
            return null;//没有这个key，get函数返回null
        } catch (Throwable t) {
            throw new RuntimeException(t);
        } finally {
            indexLock.readLock().unlock();
        }
    }

    /**
     * @描述 删除
     * @param key
     * @return void
     * @Author taoxier
     */
    @Override
    public void rm(String key) {
        try {
            RmCommand command = new RmCommand(key);
            byte[] commandBytes = JSONObject.toJSONBytes(command);
            // 加锁
            indexLock.writeLock().lock();

            //写wal
            wal.writeInt(commandBytes.length);
            wal.write(commandBytes);
            //写内存表
            memTable.put(key, command);
            //内存表过阈值就持久化
            if (memTable.size() > storeThreshold) {
                switchMemTable();
                storeSsTable();
            }
            //检查是否需要压缩
            checkIfCompress();

        } catch (Throwable t) {
            throw new RuntimeException(t);
        } finally {
            indexLock.writeLock().unlock();
        }
    }

    /**
     * @描述 关掉关掉全部关掉
     * @param
     * @return void
     * @Author taoxier
     */
    public void close() throws IOException {
        wal.close();
        for (SsTable ssTable : ssTables) {
            ssTable.close();
        }
    }
}
