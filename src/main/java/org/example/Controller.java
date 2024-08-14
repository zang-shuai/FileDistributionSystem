package org.example;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.*;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.CopyOnWriteArraySet;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;


/*
	1.	ArrayBlockingQueue：
	•	用途：这是一个基于数组的阻塞队列实现。它有一个固定的容量，并且在队列满时插入操作会被阻塞，在队列空时移除操作会被阻塞。
	•	典型用法：适用于生产者-消费者模式，其中生产者和消费者在队列上进行同步。
	2.	CopyOnWriteArraySet：
	•	用途：这是一个线程安全的 Set 实现，其中所有的写操作（添加、删除）都会创建 Set 的一个新的副本，而读取操作则是从原始副本中获取。这使得读取操作非常快，但写操作比较慢。
	•	典型用法：适用于读多写少的场景，例如缓存或事件监听器集合，读取操作较频繁且对线程安全要求高。
	3.	CountDownLatch：
	•	用途：这是一个同步辅助工具，允许一个或多个线程等待其他线程完成一组操作。它通过一个计数器来实现，线程在计数器为零时被释放。
	•	典型用法：用于协调多个线程的执行，比如在所有线程完成某项任务之前，让主线程等待。
	4.	TimeUnit：
	•	用途：这是一个枚举类，定义了不同时间单位的常量（如纳秒、微秒、毫秒、秒、分钟、小时、天）。它提供了将时间单位之间进行转换的方法，以及一些时间相关的操作方法。
	•	典型用法：用于时间计算和处理，例如设置超时时间或转换时间单位。
 */
public class Controller {
    // 当前控制器的端口号
    private static int controllerPort;
    // 表示系统中每个文件的副本的数量。即文件应该被存储在多少个 Dstore 节点上
    private static int replicaNumber;

    //  指定操作的超时时间 （毫秒）
    private static int timeout;

    // 再平衡操作的周期时间（秒）
    private static int rebalancePeriod;

    // 存储当前已连接的 Dstore 节点的端口号及其对应的 Socket 对象
    private static HashMap<Integer, Socket> dstoreMap = new HashMap<>();

    // 存储文件和它的详细信息
    private static HashMap<String, FileInfo> fileInfoMap = new HashMap<>();

    // 是否平衡
    private static boolean isRebalancing = false;

    // 用于在再平衡操作中，等待所有 Dstore 节点返回文件列表信息。同步多个 Dstore 的响应。
    private static CountDownLatch waitForAllDstoresListCommand;
    // 用于在再平衡操作中，等待某个特定 Dstore 完成文件的再平衡操作。用于同步再平衡任务的进度。
    private static CountDownLatch oneDstoreCompleteRebalance;

    /// 删除文件的请求队列，
    private static HashMap<String, ArrayBlockingQueue<Socket>> removeRequestQueue = new HashMap<>();
    private static HashMap<String, Thread> removeThread = new HashMap<>();

    /// 存储文件的请求队列，应对并发
    private static HashMap<String, ArrayBlockingQueue<Socket>> storeRequestQueue = new HashMap<>();
    private static HashMap<String, Thread> storeThread = new HashMap<>();

    /// 下载文件的请求队列，应对并发

    private static HashMap<String, ArrayBlockingQueue<LoadOrReLoadRequest>> loadRequestQueue = new HashMap<>();
    private static HashMap<String, Thread> loadThread = new HashMap<>();


    // 再平衡函数
    public static void rebalance() {
        // 记录当前时间
        var t1 = System.currentTimeMillis();

        // 标记系统进入再平衡状态，其他操作将会被阻塞
        isRebalancing = true;

        // 只有当连接的 Dstore 节点数量大于等于副本数量时，才进行再平衡操作，此时说明可以进行再平衡，否则无法保证有足够的副本
        if (dstoreMap.size() >= replicaNumber) {
            System.out.println("enter rebalance（开始再平衡操作）");

            // 对系统中每个文件进行检查，确保没有文件正在存储或删除过程中
            for (var fileName : fileInfoMap.keySet()) {
                var fileInfo = fileInfoMap.get(fileName);
                // 如果文件正在存储或删除，则暂停再平衡操作，等待这些操作完成
                while (fileInfo.status == FileStatus.STORE_IN_PROGRESS || fileInfo.status == FileStatus.REMOVE_IN_PROGRESS) {
                    // 等待，直到文件的存储或删除操作完成
                }
            }

            // 向所有 Dstore 发送 LIST_TOKEN 命令，获取它们当前存储的文件列表
            for (var dstorePort : dstoreMap.keySet()) {
                System.out.println("send list to dstore: " + dstorePort);
                Util.sendMessage(dstoreMap.get(dstorePort), Protocol.LIST_TOKEN);
            }

            // 使用 CountDownLatch 等待所有 Dstore 返回它们的文件列表
            waitForAllDstoresListCommand = new CountDownLatch(dstoreMap.size());
            try {
                // 等待 Dstore 响应，最多等待指定的超时时间
                if (waitForAllDstoresListCommand.await(timeout, TimeUnit.MILLISECONDS)) {

                    // 计算每个 Dstore 节点应存储的文件数量范围
                    // 有 n 个文件，每个文件 x 个副本，y 个结点，每个结点应有 n*x/y 个值，如果不在这个范围，则需要进行调整
                    var filesNumberInEveryDstore = (double) (replicaNumber * fileInfoMap.size()) / dstoreMap.size();
                    var low = Math.floor(filesNumberInEveryDstore);  // 最低文件数量
                    var high = Math.ceil(filesNumberInEveryDstore);  // 最高文件数量

                    // 创建一个映射，存储每个 Dstore 当前保存的文件列表
                    var filesInDstore = new HashMap<Integer, HashSet<String>>();
                    for (var fileName : fileInfoMap.keySet()) {
                        var fileInfo = fileInfoMap.get(fileName);
                        for (var dp : fileInfo.dstoresSavingFiles) {
                            filesInDstore.computeIfAbsent(dp, k -> new HashSet<>()).add(fileName);
                        }
                    }

                    // 遍历每个 Dstore，检查并调整它们的文件分布
                    for (var dstore : dstoreMap.keySet()) {
                        // 用来记录需要从该 Dstore 发送到其他 Dstore 的文件及目标 Dstore 列表
                        var filesToSendToDstore = new HashMap<String, HashSet<Integer>>();
                        // 用来记录需要从该 Dstore 删除的文件列表
                        var filesToRemoveInDstore = new HashSet<String>();

                        // 获取当前 Dstore 保存的文件列表，如果没有任何文件，则跳过
                        var files = filesInDstore.get(dstore);
                        if (files == null) continue;

                        try {
                            // 遍历每个文件，检查是否需要移动或删除
                            for (var file : files) {
                                var fileInfo = fileInfoMap.get(file);

                                // Case 1: （文件异常、不要了）如果文件未完成存储或删除，则从该 Dstore 删除该文件
                                if (fileInfo.status == null) {
                                    filesToRemoveInDstore.add(file);
                                    fileInfo.dstoresSavingFiles.remove(dstore);
                                    filesInDstore.get(dstore).remove(file);
                                    continue;
                                }

                                // Case 2: （文件过多，删除）如果该文件的副本数量超过了所需的副本数，则删除该文件的副本
                                if (fileInfo.dstoresSavingFiles.size() > replicaNumber) {
                                    filesToRemoveInDstore.add(file);
                                    fileInfo.dstoresSavingFiles.remove(dstore);
                                    filesInDstore.get(dstore).remove(file);
                                    continue;
                                }

                                // Case 3: （dstore 文件过多，不过遍历的文件副本数量刚刚达标），则移动文件
                                if (filesInDstore.get(dstore).size() - filesToRemoveInDstore.size() > high &&
                                        fileInfo.dstoresSavingFiles.size() == replicaNumber) {
                                    filesToRemoveInDstore.add(file);
                                    fileInfo.dstoresSavingFiles.remove(dstore);
                                    filesInDstore.get(dstore).remove(file);

                                    // 将文件发送到另一个 Dstore
                                    for (Integer anotherDstore : dstoreMap.keySet()) {
                                        if (anotherDstore.equals(dstore)) continue;

                                        // 如果另一个 Dstore 已经包含该文件或已满，则跳过
                                        if (filesInDstore.get(anotherDstore).contains(file) || filesInDstore.get(anotherDstore).size() > low)
                                            continue;

                                        // 添加到发送列表中
                                        filesToSendToDstore.computeIfAbsent(file, k -> new HashSet<>()).add(anotherDstore);
                                        fileInfo.dstoresSavingFiles.add(anotherDstore);
                                        filesInDstore.get(anotherDstore).add(file);
                                        break;
                                    }
                                    continue;
                                }

                                // Case 4: 如果 Dstore 文件数过多，但文件副本数不足，则移动文件
                                if (filesInDstore.get(dstore).size() - filesToRemoveInDstore.size() > high &&
                                        fileInfo.dstoresSavingFiles.size() < replicaNumber) {
                                    filesToRemoveInDstore.add(file);
                                    fileInfo.dstoresSavingFiles.remove(dstore);
                                    filesInDstore.get(dstore).remove(file);

                                    // 将文件发送到另一个 Dstore
                                    for (Integer anotherDstore : dstoreMap.keySet()) {
                                        if (anotherDstore.equals(dstore)) continue;

                                        // 如果另一个 Dstore 已经包含该文件或已满，则跳过
                                        if (filesInDstore.get(anotherDstore).contains(file) || filesInDstore.get(anotherDstore).size() > low)
                                            continue;

                                        // 添加到发送列表中
                                        filesToSendToDstore.computeIfAbsent(file, k -> new HashSet<>()).add(anotherDstore);
                                        fileInfo.dstoresSavingFiles.add(anotherDstore);
                                        filesInDstore.get(anotherDstore).add(file);
                                        if (fileInfo.dstoresSavingFiles.size() == replicaNumber) {
                                            break;
                                        }
                                    }
                                    continue;
                                }

                                // Case 5: 如果 Dstore 文件数过少，文件数量不足，则只移动文件
                                if (filesInDstore.get(dstore).size() - filesToRemoveInDstore.size() <= high &&
                                        fileInfo.dstoresSavingFiles.size() < replicaNumber) {
                                    for (Integer anotherDstore : dstoreMap.keySet()) {
                                        if (anotherDstore.equals(dstore)) continue;
                                        if (!filesInDstore.containsKey(anotherDstore)) continue;
                                        if (filesInDstore.get(anotherDstore).contains(file) || filesInDstore.get(anotherDstore).size() > low)
                                            continue;

                                        filesToSendToDstore.computeIfAbsent(file, k -> new HashSet<>()).add(anotherDstore);
                                        fileInfo.dstoresSavingFiles.add(anotherDstore);
                                        filesInDstore.get(anotherDstore).add(file);
                                        if (fileInfo.dstoresSavingFiles.size() == replicaNumber) {
                                            break;
                                        }
                                    }
                                }
                            }
                        } catch (ConcurrentModificationException e) {
                            e.printStackTrace();
                        }

                        // 构建并发送 REBALANCE 命令，包含需要发送和删除的文件列表
                        var message = new StringBuilder(Protocol.REBALANCE_TOKEN);
                        message.append(" ").append(filesToSendToDstore.size());
                        for (var file : filesToSendToDstore.keySet()) {
                            message.append(" ").append(file);
                            message.append(" ").append(filesToSendToDstore.get(file).size());
                            for (var ds : filesToSendToDstore.get(file)) {
                                message.append(" ").append(ds);
                            }
                        }
                        message.append(" ").append(filesToRemoveInDstore.size());
                        for (var fileToRemove : filesToRemoveInDstore) {
                            message.append(" ").append(fileToRemove);
                        }

                        // 如果有文件需要发送或删除，则发送命令并等待确认
                        if (!filesToSendToDstore.isEmpty() || !filesToRemoveInDstore.isEmpty()) {
                            oneDstoreCompleteRebalance = new CountDownLatch(1);
                            Util.sendMessage(dstoreMap.get(dstore), message.toString());
                            var ignored = oneDstoreCompleteRebalance.await(timeout, TimeUnit.MILLISECONDS);
                        }
                    }
                }
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }


        // 标记系统退出再平衡状态
        isRebalancing = false;

        // 记录结束时间，并打印再平衡操作花费的时间
        var t2 = System.currentTimeMillis();
        System.out.println("rebalance time: " + (t2 - t1) / 1000);
    }

    public static class DstoreHandler implements Runnable {
        // dstore 节点的端口号
        private int dstorePort;
        // 与 dstore 节点通信的 Socket 实例
        private Socket dstoreSocket;


        // 构造函数，初始化
        public DstoreHandler(int dstorePort,
                             Socket dstoreSocket) {
            this.dstorePort = dstorePort;
            this.dstoreSocket = dstoreSocket;

        }

        // 实现 Runnable 接口的 run 方法，处理来自 dstore 节点的任务
        @Override
        public void run() {
            try {
                // 获取 dstore 节点的输入流，并创建一个 BufferedReader 以读取数据
                var in = new BufferedReader(new InputStreamReader(dstoreSocket.getInputStream()));
                String inputLine;

                // 持续读取 dstore 节点发送的数据直到 dstore 关闭连接
                while ((inputLine = in.readLine()) != null) {
                    // 将接收到的行数据按空格分割成字符串数组
                    var tokens = inputLine.split(" ");

                    // 根据接收到的数据的第一个令牌进行处理
                    switch (tokens[0]) {
                        //
                        case Protocol.LIST_TOKEN -> {
                            // 处理 LIST 请求，使用虚拟线程进行异步处理
                            Thread.ofVirtual().start(() -> {
                                // 从 tokens 中提取文件名列表
                                var files = new ArrayList<>(Arrays.asList(tokens).subList(1, tokens.length));
                                for (var file : files) {
                                    if (fileInfoMap.containsKey(file)) {
                                        // 如果文件信息存在，则更新保存该文件的 dstore 列表
                                        var fileInfo = fileInfoMap.get(file);
                                        fileInfo.dstoresSavingFiles.add(dstorePort);
                                    } else {
                                        // 如果文件信息不存在，则创建新的 FileInfo 对象并添加到文件信息映射中
                                        var fileInfo = new FileInfo("0");
                                        fileInfo.status = null;
                                        fileInfoMap.put(file, fileInfo);
                                    }
                                }
                                // 文件列表处理完毕，倒计时器减一
                                waitForAllDstoresListCommand.countDown();
                            });
                        }

                        case Protocol.STORE_ACK_TOKEN -> {
                            // 处理 STORE_ACK 请求，使用虚拟线程进行异步处理
                            Thread.ofVirtual().start(() -> {
                                var fileName = tokens[1];
                                var fileInfo = fileInfoMap.get(fileName);
                                // 文件存储确认，倒计时器减一，并更新 dstore 保存文件的列表
                                fileInfo.storeLatch.countDown();
                                fileInfo.dstoresSavingFiles.add(dstorePort);
                            });
                        }

                        case Protocol.REMOVE_ACK_TOKEN, Protocol.ERROR_FILE_DOES_NOT_EXIST_TOKEN -> {
                            // 处理 REMOVE_ACK 或 ERROR_FILE_DOES_NOT_EXIST 请求，使用虚拟线程进行异步处理
                            Thread.ofVirtual().start(() -> {
                                var fileName = tokens[1];
                                var fileInfo = fileInfoMap.get(fileName);
                                // 文件移除确认，倒计时器减一，并更新 dstore 保存文件的列表
                                fileInfo.removeLatch.countDown();
                                fileInfo.dstoresSavingFiles.remove(dstorePort);
                            });
                        }

                        case Protocol.REBALANCE_COMPLETE_TOKEN -> {
                            // 处理 REBALANCE_COMPLETE 请求，倒计时器减一
                            oneDstoreCompleteRebalance.countDown();
                        }
                    }
                }

                // 当 dstore 关闭连接时，执行以下操作
                // 从 dstoreMap 中移除该 dstore 节点的记录
                dstoreMap.remove(dstorePort);

                // 遍历所有文件信息，移除 dstore 节点的相关记录
                for (var file : fileInfoMap.keySet()) {
                    var fileInfo = fileInfoMap.get(file);
                    fileInfo.loadHistory.remove(dstorePort);
                    fileInfo.dstoresSavingFiles.remove(dstorePort);
                }

                // 关闭 dstore 的 Socket 连接
                try {
                    System.out.println("close disconnected dstore socket");
                    dstoreSocket.close();
                } catch (IOException closeError) {
                    // 处理关闭 Socket 时的异常
                    closeError.printStackTrace();
                }
            } catch (IOException e) {
                // 处理读取 dstore 节点输入流时的异常
                e.printStackTrace();
            }
        }
    }

    public static void main(String[] args) {
        // 设置 Controller 端口号，8888
        controllerPort = Integer.parseInt(args[0]);
        // 设置 复制因子，代表连接了几个 dstore
        replicaNumber = Integer.parseInt(args[1]);
        // 超时时间 30
        timeout = Integer.parseInt(args[2]);
        // 再平衡时间 3，每过这个时间，开始一次再平衡操作
        rebalancePeriod = Integer.parseInt(args[3]);

        // 定时任务
        TimerTask rebalanceTask = new TimerTask() {
            @Override
            public void run() {
                // 如果isRebalancing为 false，则执行 rebalance() 函数
                if (!isRebalancing) rebalance();
            }
        };

        // rebalanceTask：要执行的任务
        // rebalancePeriod * 1000L ： 首次执行时间
        // rebalancePeriod * 1000L ： 执行周期
        new Timer("rebalance task").schedule(rebalanceTask, rebalancePeriod * 1000L, rebalancePeriod * 1000L);

        //新建一个服务端 socket
        try (var serverSocket = new ServerSocket(controllerPort)) {
            // 持续监听来自客户端的连接请求
            while (true) {
                // 接受来自客户端的连接，创建一个 Socket 实例
                Socket socket = serverSocket.accept();

                // 为每个连接创建一个新的线程来处理该连接
                new Thread(() -> {
                    try {
                        // 获取客户端输入流，并创建一个 BufferedReader 以读取输入
                        BufferedReader in = new BufferedReader(new InputStreamReader(socket.getInputStream()));
                        String inputLine;

                        // 持续读取客户端发送的数据直到客户端关闭连接
                        while ((inputLine = in.readLine()) != null) {
                            // 按空格分割接收到的行数据，得到一个字符串数组
                            String[] tokens = inputLine.split(" ");

                            // 检查接收到的token 数组中，第一个值是否为 JOIN_TOKEN，为 join 表示为加入命令
                            if (tokens[0].equals(Protocol.JOIN_TOKEN)) {
                                // 输出连接信息，表示某个 dstore 开始连接
                                System.out.println("Dstore " + tokens[1] + " connect");

                                // 获取 dstore 的端口号并解析为整数
                                int dstorePort = Integer.parseInt(tokens[1]);

                                // 将 dstore 端口号和对应的 Socket 信息存入 dstoreMap 中
                                dstoreMap.put(dstorePort, socket);

                                // 如果当前不在重新平衡状态，启动一个新的线程来执行 rebalance 方法
                                if (!isRebalancing) {
                                    new Thread(Controller::rebalance).start();
                                }

                                // 启动一个新的线程来处理与 dstore 相关的任务
                                new Thread(new DstoreHandler(dstorePort, socket)).start();

                                // 处理完 JOIN 请求后，退出当前循环
                                break;
                            } else {
                                // 处理来自客户端的其他命令
                                handleCommandFromClient(socket, inputLine);
                            }
                        }
                    } catch (IOException e) {
                        // 处理输入输出异常
                        e.printStackTrace();
                    }
                }).start();
            }
        } catch (IOException e) {
            // 处理服务器套接字创建异常
            e.printStackTrace();
        }
    }

    public static void handleCommandFromClient(Socket client, String command) {
        var tokens = command.split(" ");
        switch (tokens[0]) {
            // list命令，返回当前的文件列表
            case Protocol.LIST_TOKEN -> {
                Thread.ofVirtual().start(() -> {
                    if (dstoreMap.size() < replicaNumber) {
                        Util.sendMessage(client, Protocol.ERROR_NOT_ENOUGH_DSTORES_TOKEN);
                    } else {
                        System.out.println("LIST Start");
                        var t1 = System.currentTimeMillis();
                        while (isRebalancing) {
                        }
                        System.out.println("LIST End");
                        var t2 = System.currentTimeMillis();
                        System.out.println("list time: " + (t2 - t1) / 1000);
                        var message = new StringBuilder(Protocol.LIST_TOKEN);
                        for (var file : fileInfoMap.keySet()) {
                            var fileInfo = fileInfoMap.get(file);
                            if (fileInfo.status == FileStatus.STORE_COMPLETE) {
                                message.append(" ").append(file);
                            }
                        }
                        Util.sendMessage(client, message.toString());
                    }
                });
            }
            // 存储set
            case Protocol.STORE_TOKEN -> {
                // 如果正在平衡状态，则进行阻塞
                while (isRebalancing) {
                }
                var fileName = tokens[1];
                var fileSize = tokens[2];
                synchronized (Controller.class) {
                    // 将文件名加入到存储队列
                    if (!storeRequestQueue.containsKey(fileName)) {
                        storeRequestQueue.put(fileName, new ArrayBlockingQueue<>(10));
                    }
                    // 将文件名加入到线程队列，线程内容为存储任务，然后启动线程
                    if (!storeThread.containsKey(fileName)) {
                        var t = new Thread(() -> storeTask(fileName, fileSize));
                        storeThread.put(fileName, t);
                        t.start();
                    }
                    try {
                        //
                        storeRequestQueue.get(fileName).put(client);
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    }
                }
            }
            // 删除del
            case Protocol.REMOVE_TOKEN -> {
                while (isRebalancing) {
                }
                var fileName = tokens[1];
                synchronized (Controller.class) {
                    if (!removeRequestQueue.containsKey(fileName)) {
                        removeRequestQueue.put(fileName, new ArrayBlockingQueue<>(10));
                    }
                    if (!removeThread.containsKey(fileName)) {
                        var t = new Thread(() -> removeTask(fileName));
                        removeThread.put(fileName, t);
                        t.start();
                    }
                    try {
                        removeRequestQueue.get(fileName).put(client);
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    }
                }
            }
            // 获取get
            case Protocol.LOAD_TOKEN, Protocol.RELOAD_TOKEN -> {
                while (isRebalancing) {
                }
                var fileName = tokens[1];
                synchronized (Controller.class) {
                    if (!loadRequestQueue.containsKey(fileName)) {
                        loadRequestQueue.put(fileName, new ArrayBlockingQueue<>(10));
                    }
                    if (!loadThread.containsKey(fileName)) {
                        var t = new Thread(() -> loadTask(fileName));
                        loadThread.put(fileName, t);
                        t.start();
                    }
                    try {
                        loadRequestQueue.get(fileName).put(new LoadOrReLoadRequest(client, tokens[0]));
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    }
                }
            }
        }
    }

    public static void loadTask(String file) {
        var bq = loadRequestQueue.get(file);
        while (true) {
            try {
                var loadOrReLoadRequest = bq.take();
                if (dstoreMap.size() < replicaNumber) {
                    Util.sendMessage(loadOrReLoadRequest.socket, Protocol.ERROR_NOT_ENOUGH_DSTORES_TOKEN);
                } else if (!fileInfoMap.containsKey(file)) {
                    Util.sendMessage(loadOrReLoadRequest.socket, Protocol.ERROR_FILE_DOES_NOT_EXIST_TOKEN);
                } else if (fileInfoMap.get(file).status == FileStatus.STORE_IN_PROGRESS || fileInfoMap.get(file).status == FileStatus.REMOVE_IN_PROGRESS) {
                    Util.sendMessage(loadOrReLoadRequest.socket, Protocol.ERROR_FILE_DOES_NOT_EXIST_TOKEN);
                } else {
                    var fileInfo = fileInfoMap.get(file);
                    if (fileInfo.status == FileStatus.STORE_COMPLETE) {
                        if (loadOrReLoadRequest.command.equals(Protocol.LOAD_TOKEN)) {
                            fileInfo.loadHistory = new HashSet<>();
                        }
                        boolean isFileFound = false;
                        for (var dstorePort : fileInfo.dstoresSavingFiles) {
                            if (!fileInfo.loadHistory.contains(dstorePort)) {
                                Util.sendMessage(loadOrReLoadRequest.socket, Protocol.LOAD_FROM_TOKEN + " " + dstorePort + " " + fileInfo.size);
                                fileInfo.loadHistory.add(dstorePort);
                                isFileFound = true;
                                break;
                            }
                        }
                        if (!isFileFound) Util.sendMessage(loadOrReLoadRequest.socket, Protocol.ERROR_LOAD_TOKEN);
                    } else {
                        Util.sendMessage(loadOrReLoadRequest.socket, Protocol.ERROR_FILE_DOES_NOT_EXIST_TOKEN);
                    }
                }
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
    }

    public static void storeTask(String file, String size) {
        // 从 storeRequestQueue 中获取与该文件名对应的存储请求队列
        var bq = storeRequestQueue.get(file);

        // 无限循环，持续处理存储请求
        while (true) {
            try {
                // 从队列中取出下一个客户端的存储请求
                Socket client = bq.take();

                // 检查当前连接的 Dstore 数量是否足够副本数量（replicaNumber），如果不足，发送错误消息给客户端
                if (dstoreMap.size() < replicaNumber) {
                    Util.sendMessage(client, Protocol.ERROR_NOT_ENOUGH_DSTORES_TOKEN);
                    // 检查文件是否正在存储过程中
                } else if (fileInfoMap.containsKey(file) && fileInfoMap.get(file).status == FileStatus.STORE_IN_PROGRESS) {
                    Util.sendMessage(client, Protocol.ERROR_FILE_ALREADY_EXISTS_TOKEN);

                    // 检查文件是否已经存在
                } else if (fileInfoMap.containsKey(file) && fileInfoMap.get(file).status == FileStatus.STORE_COMPLETE) {
                    Util.sendMessage(client, Protocol.ERROR_FILE_ALREADY_EXISTS_TOKEN);

                    // 检查文件是否正在删除过程中
                } else if (fileInfoMap.containsKey(file) && fileInfoMap.get(file).status == FileStatus.REMOVE_IN_PROGRESS) {
                    Util.sendMessage(client, Protocol.ERROR_FILE_ALREADY_EXISTS_TOKEN);

                    // 如果文件不存在且没有冲突状态，则可以进行存储操作
                } else {
                    // 初始化文件名和文件大小
                    String fileName = file;

                    // 创建新的 FileInfo 对象，表示该文件的元数据，并将其存入 fileInfoMap 中
                    // fileInfoMap
                    fileInfoMap.put(fileName, new FileInfo(size));
                    // 获取新建的 FileInfo

                    // 创建一个映射，用于记录每个 Dstore 存储的文件列表（所有文件和结点！！！！）
                    var dstoreFileNameList = new HashMap<Integer, HashSet<String>>();
                    // 遍历所有文件
                    for (var filename : fileInfoMap.keySet()) {
                        // 获取其中一个文件的详细信息
                        FileInfo f = fileInfoMap.get(filename);
                        // 遍历这个文件存储的结点
                        for (var dstore : f.dstoresSavingFiles) {
                            // 如果map 中没有这个结点，则将这个结点加入到 map 中
                            if (!dstoreFileNameList.containsKey(dstore)) {
                                dstoreFileNameList.put(dstore, new HashSet<>());
                            }
                            dstoreFileNameList.get(dstore).add(fileName);
                        }
                    }

                    // 计算每个 Dstore 平均应存储的文件数量 = （文件数量*每个文件的副本数量）/结点数量
                    var filesInEveryDstore = (double) ((fileInfoMap.size() * replicaNumber) / dstoreMap.size());
                    // 上取整
                    var high = Math.ceil(filesInEveryDstore);


                    FileInfo fileInfo = fileInfoMap.get(fileName);

                    // 构建要发送给客户端的存储命令消息
                    var message = new StringBuilder(Protocol.STORE_TO_TOKEN);
                    int storeCount = 0;
                    for (var dstorePort : dstoreMap.keySet()) {
                        // 如果已经选择了足够的 Dstore 节点，则退出循环
                        if (storeCount == replicaNumber) break;

                        // 如果 Dstore 没有保存该文件，且当前结点没有存储文件，则选择该 Dstore 进行存储
                        if (!fileInfo.dstoresSavingFiles.contains(dstorePort) && dstoreFileNameList.get(dstorePort) == null) {
                            storeCount++;
                            dstoreFileNameList.put(dstorePort, new HashSet<>());
                            dstoreFileNameList.get(dstorePort).add(fileName);
                            message.append(" ").append(dstorePort);

                            // 如果 Dstore 没有保存该文件，且文件数量低于高水位限制，则选择该 Dstore 进行存储
                        } else if (!fileInfo.dstoresSavingFiles.contains(dstorePort) && dstoreFileNameList.get(dstorePort).size() < high) {
                            storeCount++;
                            dstoreFileNameList.get(dstorePort).add(fileName);
                            message.append(" ").append(dstorePort);
                        }
                    }

                    // 等待所有 Dstore 确认存储成功（store_ack），通过 CountDownLatch 实现同步
                    fileInfo.storeLatch = new CountDownLatch(replicaNumber);
                    Util.sendMessage(client, message.toString());
                    try {
                        // 等待存储确认，超时时间为 timeout 毫秒
                        if (fileInfo.storeLatch.await(timeout, TimeUnit.MILLISECONDS)) {
                            // 如果所有 Dstore 成功存储文件，更新文件状态为 STORE_COMPLETE
                            fileInfo.status = FileStatus.STORE_COMPLETE;
                            // 发送存储完成的消息给客户端
                            Util.sendMessage(client, Protocol.STORE_COMPLETE_TOKEN);
                            // 重置 CountDownLatch，以便后续操作
                            fileInfo.storeLatch = new CountDownLatch(0);
                        } else {
                            // 如果存储失败（超时），从 fileInfoMap 中移除该文件的元数据
                            fileInfoMap.remove(fileName);
                        }
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    }
                }
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
    }

    //
    public static void removeTask(String file) {
        // 从 removeRequestQueue 中获取与该文件名对应的删除请求队列
        var bq = removeRequestQueue.get(file);
        // 进入一个无限循环，持续处理删除请求
        while (true) {
            try {
                // 从请求队列中取出下一个客户端的删除请求
                Socket client = bq.take();

                // 检查当前连接的 Dstore 数量是否小于副本数，小于则不能删除
                if (dstoreMap.size() < replicaNumber) {
                    Util.sendMessage(client, Protocol.ERROR_NOT_ENOUGH_DSTORES_TOKEN);

                    // 文件不存在，发送错误信息给客户端
                } else if (fileInfoMap.get(file) == null) {
                    Util.sendMessage(client, Protocol.ERROR_FILE_DOES_NOT_EXIST_TOKEN);

                    // 正在存储过程中，无法删除
                } else if (fileInfoMap.get(file) != null && fileInfoMap.get(file).status == FileStatus.STORE_IN_PROGRESS) {
                    Util.sendMessage(client, Protocol.ERROR_FILE_DOES_NOT_EXIST_TOKEN);

                    // 文件正在删除中
                } else if (fileInfoMap.get(file) != null && fileInfoMap.get(file).status == FileStatus.REMOVE_IN_PROGRESS) {
                    Util.sendMessage(client, Protocol.ERROR_FILE_DOES_NOT_EXIST_TOKEN);

                    // 如果文件状态为存储完成（STORE_COMPLETE），则可以执行删除操作
                } else if (fileInfoMap.get(file) != null && fileInfoMap.get(file).status == FileStatus.STORE_COMPLETE) {
                    //
                    String fileName = file;
                    // 获取文件详细信息
                    var fileInfo = fileInfoMap.get(fileName);
                    // 更新文件状态为正在删除中（REMOVE_IN_PROGRESS），防止其他操作干扰
                    fileInfo.status = FileStatus.REMOVE_IN_PROGRESS;

                    // 创建一个 CountDownLatch，用于同步等待所有 Dstore 完成删除操作
                    fileInfo.removeLatch = new CountDownLatch(fileInfo.dstoresSavingFiles.size());

                    // 向保存该文件的所有 Dstore 发送删除命令
                    for (var dstorePort : fileInfo.dstoresSavingFiles) {
                        Util.sendMessage(dstoreMap.get(dstorePort), Protocol.REMOVE_TOKEN + " " + fileName);
                    }

                    try {
                        // 等待所有 Dstore 的删除确认，超时时间为 timeout 毫秒
                        if (fileInfo.removeLatch.await(timeout, TimeUnit.MILLISECONDS)) {
                            // 如果在超时时间内收到所有确认，发送删除完成的消息给客户端
                            Util.sendMessage(client, Protocol.REMOVE_COMPLETE_TOKEN);
                            // 从 fileInfoMap 中移除文件信息，表示删除已完成
                            fileInfoMap.remove(fileName);
                        } else {
                            // 如果超时，依然从 fileInfoMap 中移除文件信息，表示删除操作已完成但可能不完全成功
                            fileInfoMap.remove(fileName);
                        }
                    } catch (InterruptedException e) {
                        // 如果在等待过程中发生异常，打印堆栈跟踪信息
                        e.printStackTrace();
                    }
                }

            } catch (InterruptedException e) {
                // 如果在从队列获取请求的过程中被中断，抛出运行时异常并打印堆栈信息
                throw new RuntimeException(e);
            }
        }
    }
}





















