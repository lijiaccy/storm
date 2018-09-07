package com.surfilter.util;

import io.netty.util.internal.ConcurrentSet;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.apache.storm.hdfs.common.ModifTimeComparator;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.URI;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;

/**
 * Created by zhangxuan on 2018/7/25.
 */
public class HUtil {
    public static ArrayList<Path> listFilesByModificationTime(FileSystem fs, Path directory, ConcurrentSet set) throws IOException {
        ArrayList fstats = new ArrayList();
        RemoteIterator itr = fs.listFiles(directory,false);

        while(itr.hasNext()) {
            LocatedFileStatus result = (LocatedFileStatus)itr.next();
            if (!set.contains(result.getPath().getName()) && !result.getPath().getName().contains("COPY")){
                set.add(result.getPath().getName());
             fstats.add(result);
            }
        }

        Collections.sort(fstats, new ModifTimeComparator());
        ArrayList result1 = new ArrayList(fstats.size());
        Iterator var7 = fstats.iterator();

        while(var7.hasNext()) {
            LocatedFileStatus fstat = (LocatedFileStatus)var7.next();
            result1.add(fstat.getPath());
        }

        return result1;
    }

    public static void createNewHDFSFile(String newFile, String content) throws IOException {
        if (StringUtils.isBlank(newFile) || null == content) {
            return;
        }
        Configuration config = new Configuration();
        FileSystem hdfs = FileSystem.get(URI.create(newFile), config);
        FSDataOutputStream os = hdfs.create(new Path(newFile));
        os.write(content.getBytes("UTF-8"));
        os.close();
        hdfs.close();
    }

    public static void deleteFromHdfs(String dst) throws FileNotFoundException,IOException {
        Configuration conf = new Configuration();
        FileSystem fs = FileSystem.get(URI.create(dst), conf);
        fs.deleteOnExit(new Path(dst));
    }

    public static void main(String[] args) {
        try {
            deleteFromHdfs();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private static void deleteFromHdfs() throws FileNotFoundException,IOException {
        String dst = "hdfs://slvae:9000/lijia/mobile/qh15";
        Configuration conf = new Configuration();
        FileSystem fs = FileSystem.get(URI.create(dst), conf);
        fs.deleteOnExit(new Path(dst));
        fs.close();
    }
}
