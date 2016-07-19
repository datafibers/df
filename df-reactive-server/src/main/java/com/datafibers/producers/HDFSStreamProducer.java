package com.datafibers.producers;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;

import com.datafibers.conf.ConfigApp;
import com.datafibers.conf.ConfigHadoop;
import com.datafibers.util.FileFunc;
import com.datafibers.util.ServerFunc;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

/**
 * This class is used to write/stream data, such as metadata, to HDFS.
 * One example of usage is to stream data to HDFS. Data will first stage in a temp file.
 * Once the stream is complete, thw whole stage file will be uploaded to the HDFS.
 */

public class HDFSStreamProducer {

    public static void uploadToHDFS(String fileName) {

        try {
            FileSystem fs = FileSystem.get(ConfigHadoop.getHadoopConfig());
            String localSrc = getStageFile(fileName);

            String hdfsDest = Paths.get(ConfigApp.getHDFSLandingPath(),
                    Paths.get(fileName).getParent().toString(), Paths.get(fileName).getFileName().toString()).toString();

            ServerFunc.printToConsole("INFO","Upload local from " + localSrc + " to HDFS at "
                    + ConfigHadoop.getDefaultFSAddress() + hdfsDest);

            FileFunc.moveFromLocalFile(fs, new Path(localSrc), new Path(hdfsDest));
            fs.close();
            ServerFunc.printToConsole("INFO","FileSystem is closed after moving the stage file to HDFS");
        } catch (IOException ioe) {
            ioe.printStackTrace();
        }
    }

    public static String getStageFile(String fileName) {
        return Paths.get(ConfigApp.getServerTmp(), Paths.get(fileName).getFileName().toString()).toString();
    }

    /**
     * This is to stage the message at server tmp place using JAVA NIO without blocking
     * @param message
     */
    public static void sendMessages(String fileName, String message) {

        try {
            String stagFile = getStageFile(fileName);
            if(!Files.exists(Paths.get(stagFile))) Files.createFile(Paths.get(stagFile));
            Files.write(Paths.get(getStageFile(fileName)),
                    message.getBytes(), StandardOpenOption.APPEND);
            ServerFunc.printToConsole("INFO","Appending local staging file @" + getStageFile(fileName));

        } catch (IOException ioe) {
            ioe.printStackTrace();
        }
    }

}
