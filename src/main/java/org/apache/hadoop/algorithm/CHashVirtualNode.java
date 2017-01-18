package org.apache.hadoop.algorithm;

import org.apache.hadoop.fs.DF;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocalDirAllocator;
import org.apache.hadoop.fs.Path;

import java.io.IOException;
import java.sql.*;
import java.util.Set;
import java.util.SortedMap;
import java.util.TreeMap;

/**
 * Created by xiaoyue26 on 12/4/16.
 */
public class CHashVirtualNode implements DirAlgo {

    //FNV1_32_HASH
    private static int getHash(String str) {
        final int p = 16777619;
        int hash = (int) 2166136261L;
        for (int i = 0; i < str.length(); i++)
            hash = (hash ^ str.charAt(i)) * p;
        hash += hash << 13;
        hash ^= hash >> 7;
        hash += hash << 3;
        hash ^= hash >> 17;
        hash += hash << 5;
        // for negative
        if (hash < 0)
            hash = Math.abs(hash);
        return hash;
    }

    private final int VIRTUAL_HDDS = 5;
    private final int VIRTUAL_SSDS = 5;
    SortedMap<Integer, RealNode> virtualHdds;
    SortedMap<Integer, RealNode> virtualSsds;
    private RealNode[] hddNodes;
    private RealNode[] ssdNodes;

    private boolean preferSsd;


    public CHashVirtualNode(String contextCfgItemName) {
        if ("yarn.nodemanager.local-dirs".equals(contextCfgItemName)
                //    ) {
                || "mapreduce.cluster.local.dir".equals(contextCfgItemName)) {
            preferSsd = true;
        } else {
            preferSsd = false;
        }
        //jdbc();
    }

    @Override
    public void updateHdd(String[] hddDirs, DF[] hddDFs) {
        hddNodes = new RealNode[hddDirs.length];
        virtualHdds = new TreeMap();
        for (int i = 0; i < hddDirs.length; i++) {
            RealNode cur = new RealNode(hddDirs[i], hddDFs[i]);
            for (int j = 0; j < VIRTUAL_HDDS; j++) {
                String virtualNodeName = hddDirs[i] + "&&VN" + String.valueOf(j);
                int dirHash = getHash(virtualNodeName);
                virtualHdds.put(dirHash, cur);
            }
        }
    }

    @Override
    public void updateSsd(String[] ssdDirs, DF[] ssdDFs) {
        ssdNodes = new RealNode[ssdDirs.length];
        virtualSsds = new TreeMap();
        for (int i = 0; i < ssdDirs.length; i++) {
            RealNode cur = new RealNode(ssdDirs[i], ssdDFs[i]);
            for (int j = 0; j < VIRTUAL_SSDS; j++) {
                String virtualNodeName = ssdDirs[i] + "&&VN" + String.valueOf(j);
                int dirHash = getHash(virtualNodeName);
                virtualSsds.put(dirHash, cur);
            }
        }
    }

    private String getDir(String pathStr, long size, SortedMap<Integer, RealNode> virtualNodes) {
        int hash = getHash(pathStr);
        SortedMap<Integer, RealNode> subMap;
        RealNode realNode;
        int count = 0;
        subMap = virtualNodes.tailMap(hash);
        if (subMap.size() != 0) {
            subMap = virtualNodes;
        }
        Set<Integer> set = subMap.keySet();
        for (Integer key : set) {
            realNode = subMap.get(key);
            if (realNode.df.getAvailable() > size) {
                return realNode.dir;
            } else {
                count++;
            }
        }
        if (count < virtualNodes.size()) {
            set = virtualNodes.keySet();
            for (Integer key : set) {
                if (count >= virtualNodes.size()) {
                    break;
                }
                realNode = virtualNodes.get(key);
                if (realNode.df.getAvailable() > size) {
                    return realNode.dir;
                } else {
                    count++;
                }
            }
        }
        return null;
    }

    @Override
    public String getDir(String pathStr, long size) {
        String res;
        if (preferSsd) {
            if (ssdNodes != null) { // prefer ssd
                res = getDir(pathStr, size, virtualSsds);
                if (res != null) {
                    DebugClient.rebuild(pathStr,res);
                    return res;
                }
            }
        }
        if (hddNodes != null) {
            res = getDir(pathStr, size, virtualHdds);
            DebugClient.rebuild(pathStr,res);
            return res;
        } else {//error
            DebugClient.print("CHashVirtualNode hddNodes null");
            return null;
        }

    }

    private Path find(String pathStr, FileSystem localFS, SortedMap<Integer, RealNode> virtualNodes) throws IOException {
        int hash = getHash(pathStr);
        SortedMap<Integer, RealNode> subMap;
        RealNode realNode;
        int count = 0;
        subMap = virtualNodes.tailMap(hash);
        if (subMap.size() != 0) {
            subMap = virtualNodes;
        }
        Set<Integer> set = subMap.keySet();
        for (Integer key : set) {
            realNode = subMap.get(key);
            Path res = new Path(realNode.dir, pathStr);
            if (localFS.exists(res)) {
                return res;
            } else {
                count++;
            }
        }
        if (count < virtualNodes.size()) {
            set = virtualNodes.keySet();
            for (Integer key : set) {
                if (count >= virtualNodes.size()) {
                    break;
                }
                realNode = virtualNodes.get(key);
                Path res = new Path(realNode.dir, pathStr);
                if (localFS.exists(res)) {
                    return res;
                } else {
                    count++;
                }
            }
        }
        return null;
    }

    @Override
    public Path find(String pathStr, FileSystem localFS) throws IOException {
        Path res;
        if (preferSsd) {
            if (ssdNodes != null) {
                res = find(pathStr, localFS, virtualSsds);
                if (res != null) {
                    return res;
                }
            }
        }
        if (hddNodes != null) {
            res = find(pathStr, localFS, virtualHdds);
            return res;
        }
        return null;
    }

    @Override
    public boolean ifExists(String pathStr, FileSystem localFS) throws IOException {
        Path res;
        if (preferSsd) {
            if (ssdNodes != null) {
                res = find(pathStr, localFS, virtualSsds);
                if (res != null) {
                    return true;
                }
            }
        }
        if (hddNodes != null) {
            res = find(pathStr, localFS, virtualHdds);
            if (res != null) {
                return true;
            }
        }
        return false;
    }

    /*//  @deprecated
    private static String getDir(String pathStr, String[] localDirs) {
        // init
        SortedMap<Integer, String> virtualNodes = new TreeMap();

        for (String str : localDirs) {
            for (int i = 0; i < VIRTUAL_HDDS; i++) {
                String virtualNodeName = str + "&&VN" + String.valueOf(i);
                int dirHash = getHash(virtualNodeName);
                virtualNodes.put(dirHash, virtualNodeName);
            }
        }
        // locate pathStr
        int hash = getHash(pathStr);
        SortedMap<Integer, String> subMap =
                virtualNodes.tailMap(hash);
        Integer i = subMap.firstKey();
        String virtualNode = subMap.get(i);

        return virtualNode.substring(0, virtualNode.indexOf("&&"));
    }*/
    private String getDir(String pathStr, SortedMap<Integer, RealNode> virtualNodes) {
        int hash = getHash(pathStr);
        SortedMap<Integer, RealNode> subMap = virtualNodes.tailMap(hash);
        if (subMap.size() != 0) {
            Integer i = subMap.firstKey();
            RealNode realNode = subMap.get(i);
            return realNode.dir;
        } else {
            return virtualNodes.get(virtualNodes.firstKey()).dir;
        }
    }

    // size unknown
    public String getDir(String pathStr) {
        if (ssdNodes != null) {
            return getDir(pathStr, virtualSsds);
        } else if (hddNodes != null) {
            return getDir(pathStr, virtualHdds);
        } else {//error
            DebugClient.print("ConsistentHashing null");
            return null;
        }
    }
}
