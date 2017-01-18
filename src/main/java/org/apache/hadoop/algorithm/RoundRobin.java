package org.apache.hadoop.algorithm;

import org.apache.hadoop.fs.DF;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.IOException;

/**
 * Created by xiaoyue26 on 12/6/16.
 */
public class RoundRobin implements DirAlgo {

    private String[] ssdDirs;
    private DF[] ssdDFs;

    private String[] hddDirs;
    private DF[] hddDFs;

    private int lastHdd = 0;
    private int lastSsd = 0;

    private boolean preferSsd;

    public RoundRobin(String contextCfgItemName) {
        if ("yarn.nodemanager.local-dirs".equals(contextCfgItemName)
            //    ) {
             ||"mapreduce.cluster.local.dir".equals(contextCfgItemName)) {
            preferSsd = true;
        } else {
            preferSsd = false;
        }
    }

    @Override
    public void updateHdd(String[] h, DF[] d) {
        hddDirs = h;
        hddDFs = d;
        lastHdd = 0;
    }
    @Override
    public void updateSsd(String[] s, DF[] d) {
        ssdDirs = s;
        ssdDFs = d;
        lastSsd = 0;
    }

    @Override
    public String getDir(String pathStr, long size) {
        int i, index;
        long capacity;
        if (preferSsd) {
            index = (lastSsd + 1) % ssdDirs.length;
            for (i = 0; i < ssdDirs.length; ++i) {
                capacity = ssdDFs[index].getAvailable();
                if (capacity > size) {
                    lastSsd = index;
                    return ssdDirs[index];
                }
                index = (index + 1) % ssdDirs.length;
            }
        }
        DebugClient.print("xxxxxxxxxxxxx"+pathStr+" "+preferSsd);
        index = (lastHdd + 1) % hddDirs.length;
        for (i = 0; i < hddDirs.length; ++i) {
            capacity = hddDFs[index].getAvailable();
            if (capacity > size) {
                lastHdd = index;
                return hddDirs[index];
            }
            index = (index + 1) % hddDirs.length;
        }
        return null;
    }


    @Override
    public Path find(String pathStr, FileSystem localFS) throws IOException {
        int i, index;
        if (preferSsd) {
            index = lastSsd;
            for (i = 0; i < ssdDirs.length; ++i) {
                Path file = new Path(ssdDirs[index], pathStr);
                if (localFS.exists(file)) {
                    return file;
                }
                index = (index + 1) % ssdDirs.length;
            }
        }
        index = lastHdd;
        for (i = 0; i < hddDirs.length; ++i) {
            Path file = new Path(hddDirs[index], pathStr);
            if (localFS.exists(file)) {
                return file;
            }
            index = (index + 1) % hddDirs.length;
        }
        /*index = lastSsd;
        for (i = 0; i < ssdDirs.length; ++i) {
            Path file = new Path(ssdDirs[index], pathStr);
            if (localFS.exists(file)) {
                return file;
            }
            index = (index + 1) % ssdDirs.length;
        }*/
        return null;
    }

    @Override
    public boolean ifExists(String pathStr, FileSystem localFS) throws IOException{
        int i, index;
        if (preferSsd) {
            index = lastSsd;
            for (i = 0; i < ssdDirs.length; ++i) {
                Path file = new Path(ssdDirs[index], pathStr);
                if (localFS.exists(file)) {
                    return true;
                }
                index = (index + 1) % ssdDirs.length;
            }
        }
        index = lastHdd;
        for (i = 0; i < hddDirs.length; ++i) {
            Path file = new Path(hddDirs[index], pathStr);
            if (localFS.exists(file)) {
                return true;
            }
            index = (index + 1) % hddDirs.length;
        }
        /*index = lastSsd;
        for (i = 0; i < ssdDirs.length; ++i) {
            Path file = new Path(ssdDirs[index], pathStr);
            if (localFS.exists(file)) {
                return true;
            }
            index = (index + 1) % ssdDirs.length;
        }*/
        return false;
    }

























/*

    while (numDirsSearched < numDirs && returnPath == null) {
        long capacity = dirDF[dirNumLastAccessed].getAvailable();
        if (capacity > size) {
            returnPath = createPath(pathStr, checkWrite);
            //DebugClient.print("getLocalPathForWrite:SIZE_KNOWN:"+returnPath);
        }
        dirNumLastAccessed++;
        dirNumLastAccessed = dirNumLastAccessed % numDirs;
        numDirsSearched++;
    }*/
/*
    public synchronized Path getLocalPathToRead(String pathStr,
                                                Configuration conf) throws IOException {
        confChanged(conf);
        int numDirs = localDirs.length;
        int numDirsSearched = 0;
        //remove the leading slash from the path (to make sure that the uri
        //resolution results in a valid path on the dir being checked)
        if (pathStr.startsWith("/")) {
            pathStr = pathStr.substring(1);
        }
        while (numDirsSearched < numDirs) {
            Path file = new Path(localDirs[numDirsSearched], pathStr);
            if (localFS.exists(file)) {
                DebugClient.print("getLocalPathToRead:"+ pathStr);
                return file;
            }
            numDirsSearched++;
        }*/
}
