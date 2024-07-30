//import model.Position;
//
//import java.io.IOException;
//import java.nio.file.Files;
//import java.nio.file.Paths;
//import java.util.concurrent.locks.Lock;
//import java.util.concurrent.locks.ReentrantLock;
//import java.util.logging.Level;
//import java.util.logging.Logger;
//
//public class TableTree {
//    private static final Logger logger = Logger.getLogger(TableTree.class.getName());
//    private static final int MAX_LEVELS = 10;
//    private LevelNode[] levels;
//    private final Lock lock = new ReentrantLock();
//
//    // 压缩当前层的文件到下一层，只能被 majorCompaction() 调用
//    public void majorCompactionLevel(int level) {
//        logger.log(Level.INFO, "Compressing layer " + level + " files");
//        long start = System.currentTimeMillis();
//
//        logger.log(Level.INFO, "Compressing layer " + level + ".db files");
//
//        // 用于加载一个 SSTable 的数据区到缓存中
//        byte[] tableCache = new byte[levelMaxSize[level]];
//        LevelNode currentNode = levels[level];
//
//        // 将当前层的 SSTable 合并到一个有序二叉树中
//        SortTree memoryTree = new SortTree();
//        memoryTree.init();
//
//        lock.lock();
//        try {
//            while (currentNode != null) {
//                Table table = currentNode.table;
//
//                // 将 SSTable 的数据区加载到 tableCache 内存中
//                if (tableCache.length < table.tableMetaInfo.dataLen) {
//                    tableCache = new byte[table.tableMetaInfo.dataLen];
//                }
//                byte[] newSlice = new byte[table.tableMetaInfo.dataLen];
//
//                // 读取 SSTable 的数据区
//                try {
//                    table.f.seek(0);
//                    table.f.read(newSlice);
//                } catch (IOException e) {
//                    logger.log(Level.SEVERE, "Error reading file " + table.filePath, e);
//                    throw new RuntimeException(e);
//                }
//
//                // 读取每一个元素
//                for (Map.Entry<Key, Position> entry : table.sparseIndex.entrySet()) {
//                    Key k = entry.getKey();
//                    Position position = entry.getValue();
//                    if (!position.deleted) {
//                        Value value = kv.decode(newSlice, position.start, position.len);
//                        memoryTree.set(k, value.value);
//                    } else {
//                        memoryTree.delete(k);
//                    }
//                }
//                currentNode = currentNode.next;
//            }
//        } finally {
//            lock.unlock();
//        }
//
//        // 将 SortTree 压缩合并成一个 SSTable
//        List<Value> values = memoryTree.getValues();
//        int newLevel = level + 1;
//
//        // 目前最多支持 10 层
//        if (newLevel > MAX_LEVELS) {
//            newLevel = MAX_LEVELS;
//        }
//
//        // 创建新的 SSTable
//        createTable(values, newLevel);
//
//        // 清理该层的文件
//        LevelNode oldNode = levels[level];
//        // 重置该层
//        if (level < MAX_LEVELS) {
//            levels[level] = null;
//            clearLevel(oldNode);
//        }
//
//        long elapsedTime = System.currentTimeMillis() - start;
//        logger.log(Level.INFO, "Completed compression, consumption of time: " + elapsedTime + " ms");
//    }
//
//    // 其他方法和类的定义...
//}