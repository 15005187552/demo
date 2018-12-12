package com.kevin.hadoop.rpc;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.ipc.RPC;

import java.io.IOException;

public class Publish {
    public static void main(String[] args) throws IOException {
        RPC.Builder builder = new RPC.Builder(new Configuration())
        .setBindAddress("localhost")
        .setPort(8888)
        .setProtocol(ClientNameNodeProtocol.class)
        .setInstance(new ClientNameNodeSystem());
      RPC.Server server = builder.build();
      server.start();
    }
}
