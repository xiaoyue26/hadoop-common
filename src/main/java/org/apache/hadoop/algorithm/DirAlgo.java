package org.apache.hadoop.algorithm;

import org.apache.hadoop.fs.DF;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.IOException;

/**
 * Created by xiaoyue26 on 12/6/16.
 */
public interface DirAlgo {
    void updateHdd(String[] h, DF[] d);

    void updateSsd(String[] s, DF[] d);

    String getDir(String pathStr, long size);

    Path find(String pathStr, FileSystem localFS) throws IOException;

    boolean ifExists(String pathStr,FileSystem localFS) throws IOException;


}
