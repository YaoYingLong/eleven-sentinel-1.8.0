/*
 * Copyright 1999-2018 Alibaba Group Holding Ltd.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.alibaba.csp.sentinel.transport.command;

import com.alibaba.csp.sentinel.command.CommandHandler;
import com.alibaba.csp.sentinel.command.CommandHandlerProvider;
import com.alibaba.csp.sentinel.concurrent.NamedThreadFactory;
import com.alibaba.csp.sentinel.transport.CommandCenter;
import com.alibaba.csp.sentinel.transport.command.http.HttpEventTask;
import com.alibaba.csp.sentinel.transport.config.TransportConfig;
import com.alibaba.csp.sentinel.transport.log.CommandCenterLog;
import com.alibaba.csp.sentinel.util.StringUtil;

import java.io.IOException;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.SocketException;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.*;

/***
 * The simple command center provides service to exchange information.
 *
 * @author youji.zj
 */
public class SimpleHttpCommandCenter implements CommandCenter {
    private static final int PORT_UNINITIALIZED = -1;
    private static final int DEFAULT_SERVER_SO_TIMEOUT = 3000;
    private static final int DEFAULT_PORT = 8719;

    @SuppressWarnings("rawtypes")
    private static final Map<String, CommandHandler> handlerMap = new ConcurrentHashMap<String, CommandHandler>();

    @SuppressWarnings("PMD.ThreadPoolCreationRule")
    private ExecutorService executor = Executors.newSingleThreadExecutor(new NamedThreadFactory("sentinel-command-center-executor"));
    private ExecutorService bizExecutor;

    private ServerSocket socketReference;

    @Override
    @SuppressWarnings("rawtypes")
    public void beforeStart() throws Exception {
        // 通过SPI机制获取所有的CommandHandler实例，并保持到map中，通过@CommandMapping注解指定key名称，CommandHandler作为value
        Map<String, CommandHandler> handlers = CommandHandlerProvider.getInstance().namedHandlers();
        registerCommands(handlers); // 注册所有实现了CommandHandler的SPI接口
    }

    @Override
    public void start() throws Exception {
        int nThreads = Runtime.getRuntime().availableProcessors();
        // 创建一个cpu数线程数大小的固定线程池，用来做业务线程池
        this.bizExecutor = new ThreadPoolExecutor(nThreads, nThreads, 0L, TimeUnit.MILLISECONDS,
                new ArrayBlockingQueue<Runnable>(10),
                new NamedThreadFactory("sentinel-command-center-service-executor"),
                new RejectedExecutionHandler() {
                    @Override
                    public void rejectedExecution(Runnable r, ThreadPoolExecutor executor) {
                        CommandCenterLog.info("EventTask rejected");
                        throw new RejectedExecutionException();
                    }
                });
        // 开启服务线程池创建ServerSocket绑定端口，默认8719，实时监听sentinel-dashboard发送的请求
        Runnable serverInitTask = new Runnable() {
            int port;
            {
                try {
                    port = Integer.parseInt(TransportConfig.getPort());
                } catch (Exception e) {
                    port = DEFAULT_PORT;
                }
            }
            @Override
            public void run() {
                boolean success = false;
                ServerSocket serverSocket = getServerSocketFromBasePort(port); // 创建ServerSocket并绑定端口，端口默认从8719开始，被占用就+1
                if (serverSocket != null) {
                    CommandCenterLog.info("[CommandCenter] Begin listening at port " + serverSocket.getLocalPort());
                    socketReference = serverSocket;
                    executor.submit(new ServerThread(serverSocket));
                    success = true;
                    port = serverSocket.getLocalPort();
                } else {
                    CommandCenterLog.info("[CommandCenter] chooses port fail, http command center will not work");
                }
                if (!success) {
                    port = PORT_UNINITIALIZED;
                }
                TransportConfig.setRuntimePort(port);
                executor.shutdown();
            }
        };
        new Thread(serverInitTask).start(); // 开启服务线程
    }

    /**
     * Get a server socket from an available port from a base port.<br>
     * Increasing on port number will occur when the port has already been used.
     *
     * @param basePort base port to start
     * @return new socket with available port
     */
    private static ServerSocket getServerSocketFromBasePort(int basePort) {
        int tryCount = 0;
        while (true) {
            try {
                ServerSocket server = new ServerSocket(basePort + tryCount / 3, 100);
                server.setReuseAddress(true);
                return server;
            } catch (IOException e) {
                tryCount++;
                try {
                    TimeUnit.MILLISECONDS.sleep(30);
                } catch (InterruptedException e1) {
                    break;
                }
            }
        }
        return null;
    }

    @Override
    public void stop() throws Exception {
        if (socketReference != null) {
            try {
                socketReference.close();
            } catch (IOException e) {
                CommandCenterLog.warn("Error when releasing the server socket", e);
            }
        }
        bizExecutor.shutdownNow();
        executor.shutdownNow();
        TransportConfig.setRuntimePort(PORT_UNINITIALIZED);
        handlerMap.clear();
    }

    /**
     * Get the name set of all registered commands.
     */
    public static Set<String> getCommands() {
        return handlerMap.keySet();
    }

    class ServerThread extends Thread {
        private ServerSocket serverSocket;
        ServerThread(ServerSocket s) {
            this.serverSocket = s;
            setName("sentinel-courier-server-accept-thread");
        }
        @Override
        public void run() {
            while (true) { // 死循环监听当前端口
                Socket socket = null;
                try {
                    socket = this.serverSocket.accept();
                    setSocketSoTimeout(socket);
                    HttpEventTask eventTask = new HttpEventTask(socket); // socket事件执行线程
                    bizExecutor.submit(eventTask);
                } catch (Exception e) {
                    CommandCenterLog.info("Server error", e);
                    if (socket != null) {
                        try {
                            socket.close();
                        } catch (Exception e1) {
                            CommandCenterLog.info("Error when closing an opened socket", e1);
                        }
                    }
                    try {// In case of infinite log.
                        Thread.sleep(10);
                    } catch (InterruptedException e1) {// Indicates the task should stop.
                        break;
                    }
                }
            }
        }
    }

    @SuppressWarnings("rawtypes")
    public static CommandHandler getHandler(String commandName) {
        return handlerMap.get(commandName);
    }

    @SuppressWarnings("rawtypes")
    public static void registerCommands(Map<String, CommandHandler> handlerMap) {
        if (handlerMap != null) {
            for (Entry<String, CommandHandler> e : handlerMap.entrySet()) {
                registerCommand(e.getKey(), e.getValue());
            }
        }
    }

    @SuppressWarnings("rawtypes")
    public static void registerCommand(String commandName, CommandHandler handler) {
        if (StringUtil.isEmpty(commandName)) {
            return;
        }

        if (handlerMap.containsKey(commandName)) {
            CommandCenterLog.warn("Register failed (duplicate command): " + commandName);
            return;
        }

        handlerMap.put(commandName, handler);
    }

    /**
     * Avoid server thread hang, 3 seconds timeout by default.
     */
    private void setSocketSoTimeout(Socket socket) throws SocketException {
        if (socket != null) {
            socket.setSoTimeout(DEFAULT_SERVER_SO_TIMEOUT);
        }
    }
}
