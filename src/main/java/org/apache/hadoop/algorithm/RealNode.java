package org.apache.hadoop.algorithm;

import org.apache.hadoop.fs.DF;

/**
 * Created by xiaoyue26 on 12/6/16.
 */
public class RealNode {
    public String dir;
    public DF df;

    public RealNode(String dir, DF df) {
        this.dir = dir;
        this.df = df;
    }
}
