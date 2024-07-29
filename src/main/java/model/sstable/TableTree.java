//package model.sstable;
//import java.io.File;
//import java.io.IOException;
//import java.nio.file.Files;
//import java.util.logging.Level;
//import java.util.logging.Logger;
///**
// * @Author taoxier
// * @Date 2024/7/29 3:07
// * @描述
// */
//
//
//
//    class TableNode {
//        int index;          // 节点的索引
//        SSTable table;      // 指向 SSTable 的引用
//        TableNode next;     // 指向下一个节点
//
//        public TableNode(int index, SSTable table) {
//            this.index = index;
//            this.table = table;
//            this.next = null;
//        }
//    }
//
//     class LinkedList {
//        private TableNode head; // 链表的头节点
//
//        public LinkedList() {
//            head = null;
//        }
//
//        // 插入新节点的方法
//        public void insert(TableNode newNode) {
//            if (head == null) {
//                head = newNode; // 如果链表为空，则新节点成为头节点
//                return;
//            }
//
//            TableNode node = head;
//            while (node != null) {
//                if (node.next == null) {
//                    newNode.index = node.index + 1; // 设置新节点的索引
//                    node.next = newNode; // 将当前节点的 next 指向新节点
//                    break; // 退出循环
//                } else {
//                    node = node.next; // 移动到下一个节点
//                }
//            }
//        }
//    }
//
//
//
//    public class TableTree {
//        private static final Logger logger = Logger.getLogger(TableTree.class.getName());
//        private TableNode[] levels; // 假设 levels 是一个数组，存储每一层的 SSTable
//        private final Object lock = new Object(); // 用于线程安全
//        private static final int MAX_LEVEL = 10;
//
//        public void check() {
//            majorCompaction();
//        }
//
//        private void majorCompaction() {
//            Config con = Config.getConfig(); // 假设有一个 Config 类用于获取配置
//            for (int levelIndex = 0; levelIndex < levels.length; levelIndex++) {
//                int tableSize = getLevelSize(levelIndex) / (1000 * 1000); // 转为 MB
//                // 检查当前层的 SSTable 数量和大小
//                if (getCount(levelIndex) > con.getPartSize() || tableSize > LevelMaxSize[levelIndex]) {
//                    majorCompactionLevel(levelIndex);
//                }
//            }
//        }
//
//        private void majorCompactionLevel(int level) {
//            logger.log(Level.INFO, "Compressing layer {0} files", level);
//            long start = System.currentTimeMillis();
//            try {
//                logger.log(Level.INFO, "Compressing layer {0}.db files", level);
//                byte[] tableCache = new byte[LevelMaxSize[level]];
//                TableNode currentNode = levels[level];
//                SortTree memoryTree = new SortTree(); // 假设有一个 SortTree 类
//
//                synchronized (lock) {
//                    while (currentNode != null) {
//                        SSTable table = currentNode.table;
//                        if (tableCache.length < table.getTableMetaInfo().getDataLen()) {
//                            tableCache = new byte[table.getTableMetaInfo().getDataLen()];
//                        }
//                        byte[] newSlice = new byte[table.getTableMetaInfo().getDataLen()];
//
//                        // 读取 SSTable 的数据区
//                        try {
//                            Files.readAllBytes(new File(table.getFilePath()).toPath());
//                        } catch (IOException e) {
//                            logger.log(Level.SEVERE, "Error reading file " + table.getFilePath(), e);
//                            throw new RuntimeException(e);
//                        }
//
//                        // 读取每一个元素
//                        for (Map.Entry<Key, Position> entry : table.getSparseIndex().entrySet()) {
//                            Position position = entry.getValue();
//                            if (!position.isDeleted()) {
//                                Value value = KVD.decode(newSlice[position.getStart():(position.getStart() + position.getLen())]);
//                                memoryTree.set(entry.getKey(), value.getValue());
//                            } else {
//                                memoryTree.delete(entry.getKey());
//                            }
//                        }
//                        currentNode = currentNode.next;
//                    }
//                }
//
//                // 将 SortTree 压缩合并成一个 SSTable
//                List<Value> values = memoryTree.getValues();
//                int newLevel = level + 1;
//                if (newLevel > MAX_LEVEL) {
//                    newLevel = MAX_LEVEL;
//                }
//                createTable(values, newLevel); // 创建新的 SSTable
//                clearLevel(level);
//            } finally {
//                long elapsed = System.currentTimeMillis() - start;
//                logger.log(Level.INFO, "Completed compression, consumption of time: {0} ms", elapsed);
//            }
//        }
//
//        private void clearLevel(int level) {
//            synchronized (lock) {
//                TableNode oldNode = levels[level];
//                while (oldNode != null) {
//                    try {
//                        oldNode.table.getFile().close(); // 关闭文件
//                        Files.delete(new File(oldNode.table.getFilePath()).toPath()); // 删除文件
//                    } catch (IOException e) {
//                        logger.log(Level.SEVERE, "Error closing or deleting file " + oldNode.table.getFilePath(), e);
//                        throw new RuntimeException(e);
//                    }
//                    oldNode.table = null; // 清理引用
//                    oldNode = oldNode.next; // 移动到下一个节点
//                }
//                levels[level] = null; // 重置该层
//            }
//        }
//
//        // 假设有其他必要的方法，例如 getLevelSize、getCount、createTable 等
//    }
