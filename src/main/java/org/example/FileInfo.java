package org.example;


import java.util.HashSet;
import java.util.concurrent.CopyOnWriteArraySet;
import java.util.concurrent.CountDownLatch;

public class FileInfo {
    public String size;

    // 文件状态（3 态枚举）
    public FileStatus status;
    // 存储哪些 Dstore 节点曾经被用来加载该文件。loadHistory 是一个 HashSet 集合，用于存储加载该文件的 Dstore 节点的端口号。
    public HashSet<Integer> loadHistory;
    // 存储当前保存该文件的 Dstore 节点
    public CopyOnWriteArraySet<Integer> dstoresSavingFiles;
    // 文件存储操作的同步器、在文件开始存储时创建
    public CountDownLatch storeLatch;
    // 删除操作的同步器、在文件开始删除时创建
    public CountDownLatch removeLatch;

    public FileInfo(String size) {
        this.size = size;
        this.status = FileStatus.STORE_IN_PROGRESS;
        this.loadHistory = new HashSet<>();
        this.dstoresSavingFiles = new CopyOnWriteArraySet<>();
        this.storeLatch = new CountDownLatch(0);
        this.removeLatch = new CountDownLatch(0);
    }

    @Override
    public String toString() {
        var s = new StringBuilder();
        s.append("dstoresSavingFiles: ");
        for (var ds : dstoresSavingFiles) {
            s.append(" ").append(ds);
        }
        s.append("   loadHistory: ");
        for (var ds : loadHistory) {
            s.append(" ").append(ds);
        }
        return s.toString();
    }
}
