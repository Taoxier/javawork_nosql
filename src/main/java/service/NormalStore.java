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
import controller.SocketServerHandler;
import model.command.Command;
import model.command.CommandPos;
import model.command.RmCommand;
import model.command.SetCommand;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import utils.CommandUtil;
import utils.LoggerUtil;
import utils.RandomAccessFileUtil;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.nio.file.StandardCopyOption;
import java.util.HashMap;
import java.util.Map;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.jar.JarEntry;

public class NormalStore implements Store {

    public static final String TABLE = ".table";
    public static final String WAL = "wal";
    public static final String RW_MODE = "rw";
    public static final String NAME = "data";
    private final Logger LOGGER = LoggerFactory.getLogger(NormalStore.class);
    private final String logFormat = "[NormalStore][{}]: {}";

    private final int STORE_THRESHOLD = 1;
    private final int LOG_COMPRESSION_THRESHOLD = 1;


    /**
     * 内存表，类似缓存
     */
    private TreeMap<String, Command> memTable;

    /**
     * hash索引，存的是数据长度和偏移量
     */
    private HashMap<String, CommandPos> index;

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
    private RandomAccessFile writerReader;

    /**
     * 持久化阈值
     */
    private final int storeThreshold = STORE_THRESHOLD;

    /**
     * 日志压缩大小阈值
     */
    private final int compressionThreshold = LOG_COMPRESSION_THRESHOLD;

    public NormalStore(String dataDir) {
        this.dataDir = dataDir;
        this.indexLock = new ReentrantReadWriteLock();
        this.memTable = new TreeMap<String, Command>();
        this.index = new HashMap<>();

        File file = new File(dataDir);
        if (!file.exists()) {
            LoggerUtil.info(LOGGER, logFormat, "NormalStore", "dataDir isn't exist,creating...");
            file.mkdirs();
        }
        this.reloadIndex();

        //---------------------
        try {
            this.writerReader = new RandomAccessFile(this.genWalPath(), RW_MODE);
        } catch (FileNotFoundException e) {
            throw new RuntimeException(e);
        }
        //--------------
    }

    public String genFilePath() {
        return this.dataDir + File.separator + NAME + TABLE;
    }

    public String genWalPath() {
        return this.dataDir + File.separator + WAL + TABLE;
    }

    public String getNextLogFileGenName() {
        return this.dataDir + File.separator + NAME + "_compress" + TABLE;
    }


    //重新加载索引
    public void reloadIndex() {
        try {
            RandomAccessFile file = new RandomAccessFile(this.genFilePath(), RW_MODE);
            long len = file.length();
            long start = 0;
            file.seek(start);
            while (start < len) {
                int cmdLen = file.readInt();
                byte[] bytes = new byte[cmdLen];
                file.read(bytes);
                JSONObject value = JSON.parseObject(new String(bytes, StandardCharsets.UTF_8));
                Command command = CommandUtil.jsonToCommand(value);
                start += 4;
                if (command != null) {
                    CommandPos cmdPos = new CommandPos((int) start, cmdLen);
                    index.put(command.getKey(), cmdPos);
                }
                start += cmdLen;
            }
            file.seek(file.length());
            file.close();
        } catch (Exception e) {
            e.printStackTrace();
        }
        LoggerUtil.debug(LOGGER, logFormat, "reload index: " + index.toString());
    }

    //去重操作
    private TreeMap<String, Command> deduplicateCommands() {
        TreeMap<String, Command> latestCommands = new TreeMap<>(); // 用于存储每个key的最新命令
        try (RandomAccessFile raf = new RandomAccessFile(this.genFilePath(), RW_MODE)) {
            long length = raf.length();
            long start = 0;
            while (start < length) {
                raf.seek(start);
                int cmdLen = raf.readInt();
                byte[] cmdBytes = new byte[cmdLen];
                raf.read(cmdBytes);
                Command command = CommandUtil.jsonToCommand(JSONObject.parseObject(new String(cmdBytes, StandardCharsets.UTF_8)));
                if (command != null) {
                    // 只保留每个key的最新命令
                    latestCommands.put(command.getKey(), command);
                }
                start += 4 + cmdLen; // 移动到下一个命令
            }
        } catch (IOException e) {
            throw new RuntimeException("Error during deduplication process", e);
        }
//        LoggerUtil.debug(LOGGER,logFormat,"Deduplication");
        return latestCommands;
    }

    //实现压缩
//    private void compressLogFile() {
//        TreeMap<String, Command> latestCommands = deduplicateCommands();
//        String newLogFile = getNextLogFileGenName(); // 生成新的日志文件名
//        try (RandomAccessFile newRaf = new RandomAccessFile(newLogFile, RW_MODE)) {
//            for (Map.Entry<String, Command> entry : latestCommands.entrySet()) {
//                Command command = entry.getValue();
//                byte[] cmdBytes = JSONObject.toJSONBytes(command);
//                RandomAccessFileUtil.writeInt(newLogFile, cmdBytes.length);
//                RandomAccessFileUtil.write(newLogFile, cmdBytes);
//            }
//        } catch (IOException e) {
//            throw new RuntimeException("Error writing to the new compressed log file", e);
//        }
//
//        // 替换原日志文件
//        File originalFile = new File(genFilePath());
//        if (!originalFile.renameTo(new File(originalFile.getParent(), "backup_" + originalFile.getName()))) {
//            throw new RuntimeException("Failed to rename original log file.");
//        }
//        if (!new File(newLogFile).renameTo(originalFile)) {
//            throw new RuntimeException("Failed to rename new compressed log file to original.");
//        }
//        // 重新加载索引，以适应新的日志文件结构
//        reloadIndex();
//    }

    private void compressLogFile() {
        TreeMap<String, Command> latestCommands = deduplicateCommands();
        String newLogFile = getNextLogFileGenName(); // 生成新的日志文件名
        String tempLogFile = newLogFile + ".tmp"; // 新增临时文件名用于中间步骤

        try (RandomAccessFile newRaf = new RandomAccessFile(tempLogFile, RW_MODE)) {
            for (Map.Entry<String, Command> entry : latestCommands.entrySet()) {
                Command command = entry.getValue();
                byte[] cmdBytes = JSONObject.toJSONBytes(command);
                RandomAccessFileUtil.writeInt(tempLogFile, cmdBytes.length);
                RandomAccessFileUtil.write(tempLogFile, cmdBytes);
            }
        } catch (IOException e) {
            throw new RuntimeException("Error writing to the temporary compressed log file", e);
        }

        // 使用Files.move进行原子性重命名操作
        try {
            Files.move(Paths.get(tempLogFile), Paths.get(newLogFile), StandardCopyOption.REPLACE_EXISTING, StandardCopyOption.ATOMIC_MOVE);
        } catch (IOException e) {
            throw new RuntimeException("Failed to atomically rename temporary file to new log file", e);
        }

        // 替换原日志文件
        File originalFile = new File(genFilePath());
        File backupFile = new File(originalFile.getParent(), "backup_" + originalFile.getName());

        // 确保备份文件不冲突，这里简化处理
        if (!originalFile.renameTo(backupFile)) {
            throw new RuntimeException("Failed to rename original log file to backup.");
        }

        // 使用Files.move进行原子性替换原文件
        try {
            Files.move(Paths.get(newLogFile), Paths.get(originalFile.getAbsolutePath()), StandardCopyOption.REPLACE_EXISTING, StandardCopyOption.ATOMIC_MOVE);
        } catch (IOException e) {
            throw new RuntimeException("Failed to atomically replace original log file with compressed one", e);
        }

        // 重新加载索引，以适应新的日志文件结构
        reloadIndex();
    }


    //把内存表写入磁盘
    public void storeTable(TreeMap<String, Command> memTable) {

        File logFile = new File(this.genFilePath(), RW_MODE);
//        if (logFile.length() > compressionThreshold) {
            System.out.println("超过大小，开始压缩");
            compressLogFile();
            System.out.println("压缩结束");
//        }

        for (Map.Entry<String, Command> entry : memTable.entrySet()) {
            String key = entry.getKey();//键
            Command command = entry.getValue();//值
            byte[] commandBytes = JSONObject.toJSONBytes(command);
            RandomAccessFileUtil.writeInt(this.genFilePath(), commandBytes.length);
            int pos = RandomAccessFileUtil.write(this.genFilePath(), commandBytes);
            //添加索引
            CommandPos cmdPos = new CommandPos(pos, commandBytes.length);
            index.put(key, cmdPos);
        }
        //重置内存表
        memTable = new TreeMap<>();
    }


    @Override
    public void set(String key, String value) {
        try {
            SetCommand command = new SetCommand(key, value);
            byte[] commandBytes = JSONObject.toJSONBytes(command);
            // 加锁
            indexLock.writeLock().lock();
            // TODO://先写内存表，内存表达到一定阀值再写进磁盘
            //写wal
            writerReader.writeInt(commandBytes.length);
            writerReader.write(commandBytes);
            //写内存表
            memTable.put(key, command);

            //内存表达到一定阀值，写进磁盘
            if (memTable.size() > storeThreshold) {
                storeTable(memTable);
            }

            /*------
            // 写table（wal）文件
            RandomAccessFileUtil.writeInt(this.genFilePath(), commandBytes.length);
            int pos = RandomAccessFileUtil.write(this.genFilePath(), commandBytes);
            // 保存到memTable

            // 添加索引
            CommandPos cmdPos = new CommandPos(pos, commandBytes.length);
            index.put(key, cmdPos);
            // TODO://判断是否需要将内存表中的值写回table
            -------*/
        } catch (Throwable t) {
            throw new RuntimeException(t);
        } finally {
            //解锁
            indexLock.writeLock().unlock();
        }
    }

    @Override
    public String get(String key) {
        try {
            indexLock.readLock().lock();
            // 从索引中获取信息
            //先从内存表中找
            Command cmd;
            cmd = memTable.get(key);

            if (cmd == null) {
                //如果内存表中没有，则在索引里找
                CommandPos cmdPos = index.get(key);
                if (cmdPos == null) {
                    return null;
                }
                byte[] commandBytes = RandomAccessFileUtil.readByIndex(this.genFilePath(), cmdPos.getPos(), cmdPos.getLen());
                JSONObject value = JSONObject.parseObject(new String(commandBytes));
                cmd = CommandUtil.jsonToCommand(value);
            }

            if (cmd instanceof SetCommand) {
                return ((SetCommand) cmd).getValue();
            }
            if (cmd instanceof RmCommand) {
                return null;
            }

        } catch (Throwable t) {
            throw new RuntimeException(t);
        } finally {
            indexLock.readLock().unlock();
        }
        return null;
    }

    @Override
    public void rm(String key) {
        try {
            RmCommand command = new RmCommand(key);
            byte[] commandBytes = JSONObject.toJSONBytes(command);
            // 加锁
            indexLock.writeLock().lock();
            // TODO://先写内存表，内存表达到一定阀值再写进磁盘
            //写入内存表
            writerReader.writeInt(commandBytes.length);
            writerReader.write(commandBytes);
            memTable.put(key, command);

            if (memTable.size() > storeThreshold) {
                storeTable(memTable);
            }

            /*----------
            // 写table（wal）文件
            int pos = RandomAccessFileUtil.write(this.genFilePath(), commandBytes);
            // 保存到memTable
            // 添加索引
            CommandPos cmdPos = new CommandPos(pos, commandBytes.length);
            index.put(key, cmdPos);
            // TODO://判断是否需要将内存表中的值写回table
             -------------*/

        } catch (Throwable t) {
            throw new RuntimeException(t);
        } finally {
            indexLock.writeLock().unlock();
        }
    }

    @Override
    public void close() throws IOException {

    }
}
