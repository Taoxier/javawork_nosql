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
import model.sstable.SsTable;
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

//    private final int STORE_THRESHOLD = 1;
//    private final int LOG_COMPRESSION_THRESHOLD = 2;


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
     * hash索引，存的是数据长度和偏移量
     */
    private HashMap<String, CommandPos> index;//---------------

    /**
     * ssTable列表
     */
    private  LinkedList<SsTable> ssTables;

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
//    private final int compressionThreshold;


    public NormalStore(String dataDir, int storeThreshold, int partSize) {
        try {
            this.dataDir = dataDir;
            this.storeThreshold = storeThreshold;
            this.partSize = partSize;
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
            this.memTable = new TreeMap<String, Command>();

//            this.index = new HashMap<>();
//            this.reloadIndex();

            //从大到小加载ssTable
            TreeMap<Long, SsTable> ssTableTreeMap = new TreeMap<>(Comparator.reverseOrder());//对Long类型的键进行降序排序
            for (File file : files) {
                String fileName = file.getName();
                //如果在持久化ssTable中出现异常，则会留下WAL_TMP，需要从中恢复数据
                if (file.isFile() && fileName.equals(WAL_TMP)) {
                    restoreFromWal(new RandomAccessFile(file,RW_MODE));
                }

                //加载ssTable
                if(file.isFile()&&fileName.endsWith(TABLE)){
                    //如果是文件，并且是数据文件的话
                    int dotIndex=fileName.indexOf(".");//找到文件名中第一个点.的位置，返回点的索引
                    Long time=Long.parseLong(fileName.substring(0,dotIndex));//从文件名中提取出时间戳部分，即从文件名的开始到第一个点之间的字符，然后将其解析为一个 Long 类型的数字
                    ssTableTreeMap.put(time,SsTable.createFromFile(file.getAbsolutePath()));//放入该文件存的SsTable
                }else if(file.isFile()&&fileName.endsWith(WAL)){
                    //如果是wal文件，则加载wal
                    walFile=file;
                    wal=new RandomAccessFile(file,RW_MODE);
                    restoreFromWal(wal);
                }
            }
            ssTables.addAll(ssTableTreeMap.values());//把所有表存进SsTable
            LoggerUtil.debug(LOGGER,logFormat,"createFromFile"+ ssTables);
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
    * @描述  切换内存表  在持久化内存表时，新建一个用，存旧的内存表
    * @param
    * @return void
    * @Author taoxier
    */
    private void switchMemTable(){
        try {
            indexLock.writeLock().lock();
            //切换内存表
            immutableMemTable=memTable;//不可变内存表，暂存数据
            memTable=new TreeMap<>();
            //切换内存表的同时也切换wal
            wal.close();
            File tmpWal=new File(dataDir+WAL_TMP);
            if (tmpWal.exists()){
                if (!tmpWal.delete()){
                    //如果存在暂存wal文件，尝试删除，如果不能删则抛出异常
                    throw new RuntimeException("-[异常抛出]：删除 'tmpWal' 失败");
                }
            }
            if (!walFile.renameTo(tmpWal)){
                throw new RuntimeException("-[异常抛出]：重命名 'walFile' 变为 'tmpWal' 失败");
            }
            walFile=new File(dataDir+WAL);
            wal=new RandomAccessFile(walFile,RW_MODE);
        } catch (FileNotFoundException e) {
            throw new RuntimeException(e);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }finally {
            indexLock.writeLock().unlock();
            LoggerUtil.debug(LOGGER,logFormat,"switchMemTable");
        }
    }

    /**
    * @描述  把内存表持久化到SsTable
    * @param
    * @return void
    * @Author taoxier
    */
private void storeSsTable(){
    try {
        SsTable ssTable=SsTable.createFromMemTable(dataDir+System.currentTimeMillis()+TABLE,partSize,immutableMemTable);//按照时间命名 创内存表对应的ssTable
        ssTables.addFirst(ssTable);//插在开头

        //存完了可以重置暂存内存表和删临时Wal
        immutableMemTable=null;
        File tmpWal=new File(dataDir+WAL_TMP);
        if (tmpWal.exists()){
            if (!tmpWal.delete()){
                throw new RuntimeException("-[异常抛出]：删除 'tmpWal' 失败");
            }
        }
    } catch (RuntimeException e) {
        throw new RuntimeException(e);
    }
}

    //把内存表写入磁盘
//    public void storeTable(TreeMap<String, Command> memTable) {
//        try {
//
//            for (Map.Entry<String, Command> entry : memTable.entrySet()) {
//                String key = entry.getKey();//键
//                Command command = entry.getValue();//值
//                byte[] commandBytes = JSONObject.toJSONBytes(command);
//                RandomAccessFileUtil.writeInt(this.genFilePath(), commandBytes.length);
//                int pos = RandomAccessFileUtil.write(this.genFilePath(), commandBytes);
//                //添加索引
//                CommandPos cmdPos = new CommandPos(pos, commandBytes.length);
//                index.put(key, cmdPos);
//            }
//        } catch (Throwable t) {
//            throw new RuntimeException(t);
//        }
//        //重置内存表
//        memTable = new TreeMap<>();
//
//        File logFile = new File(this.genFilePath(), "r");
////        if (logFile.length() > compressionThreshold) {
////        System.out.println("超过大小，开始压缩");
//        compressLogFile();
////        System.out.println("压缩结束");
//
////        }
//
//    }


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
