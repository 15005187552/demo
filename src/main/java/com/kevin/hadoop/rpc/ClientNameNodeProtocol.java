package com.kevin.hadoop.rpc;

public interface ClientNameNodeProtocol {
   public static final long versionID=1L;
   public String getMetaData(String path);
}
