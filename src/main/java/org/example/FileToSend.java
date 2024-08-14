package org.example;

import java.util.List;

public class FileToSend {
    // 文件名
    String fileName;
    // 文件要发送的集合
    List<String> dstores;

    public FileToSend(String fileName, List<String> dstores) {
        this.fileName = fileName;
        this.dstores = dstores;
    }

    @Override
    public String toString() {
        return "FileToSend{" +
                "fileName='" + fileName + '\'' +
                ", dstores=" + dstores +
                '}';
    }
}
