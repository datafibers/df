package io.example.vertx.util;

import org.apache.hadoop.fs.FileSystem;

import java.io.IOException;


public class FileFunc {

    /**
     * This method moves the file from DF Server to HDFS while keeping a suffix (_COPYING_) before file is done.
     *
     * @param fs
     *            FileSystem object
     * @param src
     *            source file
     * @param dst
     *            final destination file
     * @throws IOException
     */
    public static void moveFromLocalFile(FileSystem fs, org.apache.hadoop.fs.Path src, org.apache.hadoop.fs.Path dst)
            throws IOException {
        //Assume hdfs landing folder is always there. Append file's parent folder after it
        if(!fs.exists(dst.getParent())) fs.mkdirs(dst.getParent());

        org.apache.hadoop.fs.Path tmp_dst = new org.apache.hadoop.fs.Path(dst.getParent(),
                dst.getName() + "_COPYING_");
        fs.moveFromLocalFile(src, tmp_dst);

        try {
            // clean up the existing file first as it might allow duplicates
            fs.delete(dst, true);
            // rename the file to final file
            fs.rename(tmp_dst, dst);
        } catch (IOException e) {
            // in case of any issue, delete the file and pop up the exception
            fs.delete(tmp_dst, true);
            throw e;
        }
    }
}
