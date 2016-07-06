package io.example.producers;

import java.io.BufferedInputStream;
import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;

/**
 * Producer used to sink streamed data to HDFS
 */
public class HDFSStreamProducer {

    public static void main(String[] args) throws IOException {
        // TODO Auto-generated method stub
        Configuration conf = new Configuration();
        conf.addResource(new Path("/path/to/your/hadoop/directory/conf/core-site.xml"));
        conf.addResource(new Path("/path/to/your/hadoop/directory/conf/hdfs-site.xml"));
        FileSystem fs = FileSystem.get(conf);
        FSDataOutputStream out = fs.append(new Path("/demo.txt"));
        out.writeUTF("Append demo...");
        fs.close();

    }

}
