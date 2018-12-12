package com.kevin.hadoop.rpc.client;

import com.kevin.hadoop.rpc.ClientNameNodeProtocol;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.ipc.RPC;

import java.io.IOException;
import java.net.InetSocketAddress;

public class Client {
    public static void main(String[] args) throws IOException {
        ClientNameNodeProtocol nameNodeProtocol = RPC.getProxy(ClientNameNodeProtocol.class, 1L, new InetSocketAddress("localhost",8888), new Configuration());
        String metaData = nameNodeProtocol.getMetaData("/kevin");
        System.out.println(metaData);
    }
}
