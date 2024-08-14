package org.example;

import java.util.List;

public class FilesToSendAndToRemove {
    // 存储该文件要发送到的dstore
    public List<FileToSend> filesToSendList;
    // 要移除的文件
    public List<String> filesToRemoveList;

    public FilesToSendAndToRemove(List<FileToSend> filesToSendList, List<String> filesToRemoveList) {
        this.filesToSendList = filesToSendList;
        this.filesToRemoveList = filesToRemoveList;
    }
}