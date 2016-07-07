package io.example.producers;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;

import io.example.conf.ConfigApp;
import io.example.conf.ConfigHadoop;
import io.example.vertx.util.FileFunc;
import io.example.vertx.util.ServerFunc;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

/**
 * Producer used to sink streamed data to HDFS
 */
public class HDFSStreamProducer {

    public static void uploadToHDFS(String fileName) {

        try {
            FileSystem fs = FileSystem.get(ConfigHadoop.getHadoopConfig());
            String localSrc = getStageFile(fileName);

            String hdfsDest = Paths.get(ConfigApp.getHDFSLandingPath(),
                    Paths.get(fileName).getParent().toString(), Paths.get(fileName).getFileName().toString()).toString();

            ServerFunc.printToConsole("INFO","localSrc is " + localSrc);
            ServerFunc.printToConsole("INFO","hdfsDest is " + hdfsDest);

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
            ServerFunc.printToConsole("INFO","Create|Append staging local file at" +
                    ConfigApp.getServerTmp()+ Paths.get(fileName).getFileName());

        } catch (IOException ioe) {
            ioe.printStackTrace();
        }
    }

}
