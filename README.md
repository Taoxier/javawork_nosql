# 实现kv型数据库

## 基于内存索引的kv数据库
### 需求分析
数据库的基本功能：
- 增
- 删
- 改
- 查
- 提供client（API）供别人使用（基于C/S架构）

数据库设计：
基于append log的方式实现。
- 参考WAL或MySQL的RedoLog。
- 除了查询操作，增、删、改都可以通过在log最后append一个命令来实现；
- 将增和改合并成一个命令set，删独立一个命令rm，查询独立一个命令get。
- 由于查询操作不需要写磁盘（持久化），我们只需要记录set和rm命令

log的数据结构设计：
通过追加log的方式实现，我们可以记录命令的方式进行，而不是记录原始数据。如：
```
{"key":"zsy1","type":"SET","value":"1"}
{"key":"zsy1","type":"RM","value":"1"}
```
优点：
- 可方便的实现删除的功能（标记删除）。查询的时候，如果查到某个key的数据，type=RM时，数据该数据已被删除
- 可实现Redo的功能（回放），只需要将记录到磁盘的命令重新执行一次即可。

索引问题：
- 基于内存的索引，服务重启时，索引丢失。
- 数据库启动时，需要通过回放功能，把磁盘中的命令redo一次来刷新索引到内存。
- 缺点：数据冷启动时，磁盘数据越大，启动时间越长。
- 上述设计，存命令，仍存在问题，磁盘中没有数据长度，redo操作就无法实现。
```38{"key":"zsy1","type":"SET","value":"1"}
38{"key":"zsy2","type":"RM","value":"1"}
```


API设计：
- 基于C/S架构
- 使用Socket实现
- 也可以使用serverlet实现Restful API（加分）
- 提供命令行Client


优化：
- 日志文件压缩
- 引入内存缓存，实现数据的批量写入
- 实现lsmt

## 实现

### java中数据命令的定义如下
rm:
```java
@Setter
@Getter
public class RmCommand extends AbstractCommand {
    private String key;

    public RmCommand(String key) {
        super(CommandTypeEnum.RM);
        this.key = key;
    }
}
```
set:
```java
@Setter
@Getter
public class SetCommand extends AbstractCommand {
    private String key;

    private String value;

    public SetCommand(String key, String value) {
        super(CommandTypeEnum.SET);
        this.key = key;
        this.value = value;
    }
}

```

### 数据写入
```java
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
            //如果还找不到，可能处于压缩过程，从暂存ssTables列表中找
            if (command == null&&immutableSsTables!=null) {
                for (SsTable ssTable : immutableSsTables) {
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
```
