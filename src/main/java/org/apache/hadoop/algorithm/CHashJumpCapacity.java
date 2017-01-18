package org.apache.hadoop.algorithm;

import org.apache.hadoop.fs.DF;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.IOException;

/**
 * Created by xiaoyue26 on 12/21/16.
 */
public class CHashJumpCapacity implements DirAlgo {

    String hddDirs[];
    DF[] hddDFs;
    long[] hddSum;

    String ssdDirs[];
    DF[] ssdDFs;
    long[] ssdSum;

    boolean preferSsd;


    public CHashJumpCapacity(String contextCfgItemName) {
        if ("yarn.nodemanager.local-dirs".equals(contextCfgItemName)
                //    ) {
                || "mapreduce.cluster.local.dir".equals(contextCfgItemName)) {
            preferSsd = true;
        } else {
            preferSsd = false;
        }

    }

    private static long getHash(String str) {
        // change hash function here:
        return GeneralHashFunctionLibrary.APHash(str);
    }

    @Override
    public void updateHdd(String[] h, DF[] d) {
        hddDirs = h;
        hddDFs = d;
        if (d == null || d.length == 0) {
            return;
        }
        hddSum = new long[d.length+1];
        hddSum[0] = d[0].getAvailable();
        for (int i = 1; i < d.length; i++) {
            hddSum[i] = hddSum[i - 1] + d[i].getAvailable();
        }
        hddSum[d.length]=hddSum[d.length-1]+1;
    }

    @Override
    public void updateSsd(String[] s, DF[] d) {
        ssdDirs = s;
        ssdDFs = d;
        if (d == null || d.length == 0) {
            return;
        }
        ssdSum = new long[d.length+1];
        ssdSum[0] = d[0].getAvailable();
        for (int i = 1; i < d.length; i++) {
            ssdSum[i] = ssdSum[i - 1] + d[i].getAvailable();
        }
        ssdSum[d.length]=ssdSum[d.length-1]+1;
    }

    private String getDir(long input, String pathStr, long size, String[] dirs, DF[] dfs, long[] sum) {
        int index = JumpConsistentHash.consistentHash(input, dirs.length, sum);
        for (int i = 0; i < dirs.length; ++i) {
            if (dfs[index].getAvailable() > size) {
                DebugClient.rebuild(pathStr, dirs[index]);
                return dirs[index];
            }
            index = (index + 1) % dirs.length;
        }
        DebugClient.print("xxxxxxxx:getDir return null");
        return null;
    }

    @Override
    public String getDir(String pathStr, long size) {
        long input = getHash(pathStr);
        String res;
        if (preferSsd) {
            res = getDir(input, pathStr, size, ssdDirs, ssdDFs, ssdSum);
            if (res != null) {
                return res;
            }
        }
        DebugClient.print("get ssd failed:"+pathStr);
        return getDir(input, pathStr, size, hddDirs, hddDFs, hddSum);
    }

    private Path find(long input, String pathStr, FileSystem localFS, String[] dirs, DF[] dfs,long []sum) throws IOException {
        int index = JumpConsistentHash.consistentHash(input, dirs.length,sum);
        for (int i = 0; i < dirs.length; ++i) {
            Path path = new Path(dirs[index], pathStr);
            if (localFS.exists(path)) {
                return path;
            }
            index = (index + 1) % dirs.length;
        }
        DebugClient.print("didn't find" + pathStr);
        return null;
    }

    @Override
    public Path find(String pathStr, FileSystem localFS) throws IOException {
        long input = getHash(pathStr);
        Path res;
        if (preferSsd) {
            res = find(input, pathStr, localFS, ssdDirs, ssdDFs,ssdSum);
            if (res != null) {
                return res;
            }
        }
        return find(input, pathStr, localFS, hddDirs, hddDFs,hddSum);
    }


    @Override
    public boolean ifExists(String pathStr, FileSystem localFS) throws IOException {
        long input = getHash(pathStr);
        Path res;
        if (preferSsd) {
            res = find(input, pathStr, localFS, ssdDirs, ssdDFs,ssdSum);
            if (res != null) {
                return true;
            }
        }
        res = find(input, pathStr, localFS, hddDirs, hddDFs,hddSum);
        if (res != null) {
            return true;
        }
        DebugClient.print("no exists " + pathStr);
        return false;
    }
}
