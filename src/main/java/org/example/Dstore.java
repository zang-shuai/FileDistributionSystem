package org.example;

import java.io.*;
import java.net.*;
import java.nio.file.DirectoryNotEmptyException;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

public class Dstore {
    // 本节点的端口号
    private static int port;
    // 控制器端口号
    private static int cport;
    // 超时时间
    private static int timeout;
    // 对应的文件夹、收到的文件都在这个文件夹中。
    private static String fileFolder;
    // 对应的文件夹类、封装上述文件夹路径
    private static File dir;
    // 拥有的文件列表，元素为文件名
    private static ArrayList<String> filesInDstore;
    // 控制器节点建立的Socket连接，用于接收来自控制器的指令和发送状态信息
    private static Socket controllerConnection;
    // 用来存储文件名和其大小的映射关系，方便快速查找文件大小，键为文件名、值为文件大小
    private static ConcurrentHashMap<String, Integer> fileSizes = new ConcurrentHashMap<>();
    // 用于控制文件发送操作的同步，确保在发送文件之前接收到了来自目标客户端节点的确认
    private static CountDownLatch waitForSendFileToDstore;

    public static void main(String[] args) {
        if (args.length != 4) {
            throw new RuntimeException("wrong number of args");
        }
        port = Integer.parseInt(args[0]);
        cport = Integer.parseInt(args[1]);
        timeout = Integer.parseInt(args[2]);
        fileFolder = args[3];
        filesInDstore = new ArrayList<>();
        dir = new File(fileFolder);

        // 先清空文件夹
        cleanDirectory(dir);

        // 首先连接到controller
        new Thread(Dstore::ConnectionToController).start();


        // 不断监听controller的信息
        try {
            // 新建一个服务端套接字
            ServerSocket serverSocket = new ServerSocket(port);
            while (true) {
                try {
                    // 从服务端获取连接信息
                    Socket clientSocket = serverSocket.accept();
                    clientSocket.setSoTimeout(timeout);
                    // 获取到连接后，新建一个线程：
                    new Thread(() -> {
                        try {
                            // 获取输入进来的命令流
                            BufferedReader in = new BufferedReader(new InputStreamReader(clientSocket.getInputStream()));
                            String line;
                            while ((line = in.readLine()) != null) {
                                var words = line.split(" ");
                                var command = words[0];
                                // 解析命令
                                switch (command) {
                                    // 如果为存储、平衡命令，
                                    case Protocol.STORE_TOKEN, Protocol.REBALANCE_STORE_TOKEN -> {
                                        // 接收文件
                                        receiveFile(clientSocket, words, dir, controllerConnection);
                                    }
                                    // 如果为加载数据命令
                                    case Protocol.LOAD_DATA_TOKEN -> {
                                        // 如果没有该文件、则：
                                        if (!filesInDstore.contains(words[1])) {
                                            clientSocket.close();
                                        }
                                        // 发送文件
                                        sendFile(clientSocket, words[1]);
                                    }
                                    default -> System.out.println("Malformed message received: " + line);
                                }
                            }
                        } catch (Exception e) {
                            e.printStackTrace();
                        }
                    }).start();
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private static void ConnectionToController() {
        try {
            // 创建一个socket，并向服务器发送join命令
            controllerConnection = new Socket(InetAddress.getLocalHost(), cport);
            Util.sendMessage(controllerConnection, Protocol.JOIN_TOKEN + " " + port);

            // 处理返回的信息
            try {
                while (true) {
                    try {
                        // 获取controller返回的数据
                        var in = new BufferedReader(new InputStreamReader(controllerConnection.getInputStream()));
                        String line;
                        // 分析返回的命令
                        while ((line = in.readLine()) != null) {
                            var words = line.split(" ");
                            var command = words[0];
                            // 解析得到的命令
                            switch (command) {
                                // 获取文件列表、返回
                                case Protocol.LIST_TOKEN -> listFilesInDstore(controllerConnection);
                                // 删除文件
                                case Protocol.REMOVE_TOKEN -> removeFileInDstore(words[1], controllerConnection);
                                // 再平衡
                                case Protocol.REBALANCE_TOKEN -> {
                                    // files_to_send ：要发送的文件列表
                                    FilesToSendAndToRemove t = parseSendFilesAndRemoveFiles(line);
                                    // 获取要发送的文件列表，列表的值为文件名和对应的 dstore 列表
                                    List<FileToSend> filesToSend = t.filesToSendList;

                                    for (var fileToSend : filesToSend) {
                                        Integer fileSize = fileSizes.get(fileToSend.fileName);
                                        // 遍历要发送的 dstore 列表
                                        for (var dstorePort : fileToSend.dstores) {
                                            // 新建一个 socket，用于向另一个 dstore 发送文件
                                            var dstoreSocket = new Socket(InetAddress.getLocalHost(), Integer.parseInt(dstorePort));
                                            // 发送 Rebalance Store 命令和文件信息
                                            Util.sendMessage(dstoreSocket, Protocol.REBALANCE_STORE_TOKEN + " " + fileToSend.fileName + " " + fileSize);
                                            // 创建一个 CountDownLatch 用于同步等待文件发送的确认
                                            waitForSendFileToDstore = new CountDownLatch(1);
                                            // 创建并启动一个虚拟线程，监听来自 dstore 的 ACK 确认消息
                                            Thread.ofVirtual().start(() -> sendFileToDstore(dstoreSocket));
                                            // 等待 ACK，如果在超时前收到 ACK，发送文件
                                            if (waitForSendFileToDstore.await(timeout, TimeUnit.MILLISECONDS)) {
                                                sendFile(dstoreSocket, fileToSend.fileName);
                                            }
                                            // 关闭 socket
                                            dstoreSocket.close();
                                        }
                                    }

                                    var filesToRemove = t.filesToRemoveList;
                                    for (var fileToRemove : filesToRemove) {
                                        if (filesInDstore.contains(fileToRemove)) {
                                            var file = new File(dir, fileToRemove);
                                            if (file.delete()) {
                                                filesInDstore.remove(fileToRemove);
                                            }
                                        }
                                    }

                                    Util.sendMessage(controllerConnection, Protocol.REMOVE_COMPLETE_TOKEN);
                                }
                                default -> System.out.println("Malformed Message");
                            }
                        }
                    } catch (Exception e) {
                        e.printStackTrace();
                    }
                }
            } catch (Exception e) {
                e.printStackTrace();
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    // 读取dstoreSocket中的输入流，当接收到了 ACK 时 触发同步机制，通知其他线程文件传输已完成或可以继续进行
    private static void sendFileToDstore(Socket dstoreSocket) {
        try {
            var in = new BufferedReader(new InputStreamReader(dstoreSocket.getInputStream()));
            String line;
            while ((line = in.readLine()) != null) {
                var parts = line.split(" ");
                if (parts[0].equals(Protocol.ACK_TOKEN)) {
                    waitForSendFileToDstore.countDown();
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    // 返回当前的文件列表到 socket
    private static void listFilesInDstore(Socket controllerConnection) {
        var msg = new StringBuilder(Protocol.LIST_TOKEN);
        for (var file : filesInDstore) {
            msg.append(" ").append(file);
        }
        Util.sendMessage(controllerConnection, msg.toString());
    }

    // 删除文件，删除完成后回应controllerConnection
    private static void removeFileInDstore(String fileName, Socket controllerConnection) {
        if (filesInDstore.contains(fileName)) {
            var toRemove = new File(dir, fileName);

            if (toRemove.delete()) {
                filesInDstore.remove(fileName);
                Util.sendMessage(controllerConnection, Protocol.REMOVE_ACK_TOKEN + " " + fileName);
            }
        } else {
            Util.sendMessage(controllerConnection, Protocol.ERROR_FILE_DOES_NOT_EXIST_TOKEN + " " + fileName);
        }
    }

    // 将文件fileName发送到对应的 socket
    private static void sendFile(Socket socket, String fileName) {
        // 使用try-with-resources自动管理资源
        try (var ignored = new FileInputStream(new File(dir, fileName));
        ) {
            var dataOut = new DataOutputStream(socket.getOutputStream());
            // 直接使用Files.readAllBytes来读取文件内容到字节数组
            byte[] fileContent = Files.readAllBytes(new File(dir, fileName).toPath());
            dataOut.write(fileContent);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    /*
     * 解析命令：___ 文件数量 发送文件名1 端口1 端口2 端口3 删除文件1 删除文件2 删除文件3
     * 仅解析命令，并将命令封装成：FilesToSendAndToRemove
     */
    public static FilesToSendAndToRemove parseSendFilesAndRemoveFiles(String line) {
        // 将接收到的指令字符串按空格分割成多个部分
        String[] parts = line.split(" ");
        // 初始化索引，用于遍历parts数组
        int index = 1; // 从第二个元素开始，因为第一个元素是协议的命令
        // 解析发送文件的数量
        int numberOfFilesToSend = Integer.parseInt(parts[index++]);
        // 创建一个列表，用于存储需要发送的文件信息
        List<FileToSend> filesToSendList = new ArrayList<>();

        // 解析需要发送的文件信息
        for (int i = 0; i < numberOfFilesToSend; i++) {
            // 获取文件名
            String fileName = parts[index++];
            // 获取目标Dstore的数量
            int numberOfDstores = Integer.parseInt(parts[index++]);
            // 创建一个列表，用于存储每个文件应该发送到的Dstore端口
            List<String> dstores = new ArrayList<>();
            // 解析每个文件应该发送到的Dstore端口
            for (int j = 0; j < numberOfDstores; j++) {
                dstores.add(parts[index++]);
            }
            // 创建FileToSend对象，并添加到列表中
            filesToSendList.add(new FileToSend(fileName, dstores));
        }

        // 解析需要删除的文件列表
        // 再次解析数量，这次是删除的文件数量
        int numberOfFilesToRemove = Integer.parseInt(parts[index++]);
        // 创建一个列表，用于存储需要删除的文件名
        List<String> filesToRemoveList = new ArrayList<>();
        for (int i = 0; i < numberOfFilesToRemove; i++) {
            // 获取并添加需要删除的文件名
            filesToRemoveList.add(parts[index++]);
        }

        // 创建并返回FilesToSendAndToRemove对象，包含两个列表
        return new FilesToSendAndToRemove(filesToSendList, filesToRemoveList);
    }

    /*
     * 清理dir目录下的文件
     */
    public static void cleanDirectory(File dir) {
        // 检查传入的目录是否存在
        if (!dir.exists()) {
            // 如果目录不存在，尝试创建它
            boolean dirsCreated = dir.mkdirs();
            // 如果目录创建失败，打印错误消息并返回
            if (!dirsCreated) {
                System.err.println("Failed to create directories: " + dir.getAbsolutePath());
                return;
            }
        }

        // 获取目录中的所有文件和文件夹
        File[] files = dir.listFiles();
        // 如果存在文件或文件夹，遍历数组
        if (files != null) {
            for (File f : files) {
                // 对于每个文件或文件夹，尝试删除它
                // Files.delete方法用于删除文件或目录，如果传入的是目录，则会抛出异常
                try {
                    Files.delete(f.toPath());
                } catch (DirectoryNotEmptyException e) {
                    // 如果目录不为空，打印错误消息
                    System.err.println("Directory not empty and cannot be deleted: " + f.getAbsolutePath());
                } catch (IOException e) {
                    // 如果删除过程中发生其他IO异常，打印异常信息
                    e.printStackTrace();
                }
            }
        }
    }

    /*
     * 传入客户端 socket 和控制器 socket，命令，文件夹，解析命令，将文件通过clientSocket返回，完成后，回应controllerConnection。
     *
     * clientSocket:与客户端通信
     * words:命令数组
     * dir:要放入的文件目录
     * controllerConnection:与控制器通信
     */
    public static void receiveFile(Socket clientSocket, String[] words, File dir, Socket controllerConnection) {
        // 从传入的words数组中获取文件名
        String fileName = words[1];
        // 从words数组中获取文件大小，并转换为整数
        int fileSize = Integer.parseInt(words[2]);
        // 根据文件名创建一个文件对象，这个文件将用于存储接收到的文件内容
        File outputFile = new File(dir, fileName);

        // 使用try-with-resources语句自动管理资源，确保所有资源在使用后都被正确关闭
        try (InputStream fileInStream = clientSocket.getInputStream(); // 获取客户端Socket的输入流
             FileOutputStream out = new FileOutputStream(outputFile)) { // 创建一个文件输出流，用于将数据写入文件

            // 向客户端发送确认消息，表示准备接收文件
            Util.sendMessage(clientSocket, Protocol.ACK_TOKEN);

            // 创建一个字节缓冲区用于读取数据
            byte[] buffer = new byte[4096]; // 缓冲区大小为4096字节
            int bytesRead; // 用于存储每次读取的字节数

            // 循环读取输入流直到文件传输完成
            while ((bytesRead = fileInStream.read(buffer)) != -1) {
                // 将读取到的数据写入文件
                out.write(buffer, 0, bytesRead);
            }

            // 根据接收到的命令类型发送不同的确认消息给控制器
            if (Protocol.STORE_TOKEN.equals(words[0])) {
                // 如果命令是STORE_TOKEN，发送存储确认消息
                Util.sendMessage(controllerConnection, Protocol.STORE_ACK_TOKEN + " " + fileName);
            }

            // 更新Dstore的文件列表和文件大小映射
            filesInDstore.add(fileName); // 将文件名添加到文件列表中
            fileSizes.put(fileName, fileSize); // 将文件名和大小添加到文件大小映射中

        } catch (Exception e) {
            // 捕获并打印可能发生的任何异常
            e.printStackTrace();
        }
    }
}