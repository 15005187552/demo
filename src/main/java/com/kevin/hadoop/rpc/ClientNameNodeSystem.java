package com.kevin.hadoop.rpc;

public class ClientNameNodeSystem implements ClientNameNodeProtocol{

    @Override
    public String getMetaData(String path) {
          return path + "blok01:{datanode01,datanode02,datanode03}";
    }
}
