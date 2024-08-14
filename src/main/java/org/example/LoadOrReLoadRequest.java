package org.example;

import java.net.Socket;

class LoadOrReLoadRequest {
    public Socket socket;
    public String command;

    public LoadOrReLoadRequest(Socket socket, String command) {
        this.socket = socket;
        this.command = command;
    }
}