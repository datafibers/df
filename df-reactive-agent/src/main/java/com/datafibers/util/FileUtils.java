package com.datafibers.util;

import java.io.File;
import java.io.IOException;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

import static java.nio.file.StandardCopyOption.REPLACE_EXISTING;

/**
 * The FileUtils calss is used to offer different filters when watching and processing the files for agent
 */
public class FileUtils {

    public static DirectoryStream.Filter<Path> filterOutStartWith(String startStrToFilterOut) {
        return new DirectoryStream.Filter<Path>() {
            @Override
            public boolean accept(Path entry) throws IOException {
                String fileName = entry.getFileName().toString();
                return fileName != null && !fileName.startsWith(startStrToFilterOut);
            }
        };
    }

    public static DirectoryStream.Filter<Path> filterOutStartWith() {
        return new DirectoryStream.Filter<Path>() {
            @Override
            public boolean accept(Path entry) throws IOException {
                String fileName = entry.getFileName().toString();
                return fileName != null
                        && !fileName.startsWith(AgentConstant.IGNORE_FILES_POSTFIX)
                        && !fileName.endsWith(AgentConstant.PROCESSING_FILES_POSTFIX)
                        && !fileName.endsWith(AgentConstant.PROCESSED_FILES_POSTFIX);
            }
        };
    }

    public static DirectoryStream.Filter<Path> filterHidden() {
        return new DirectoryStream.Filter<Path>() {
            @Override
            public boolean accept(Path entry) throws IOException {
                File file = entry.toFile();
                return !file.isHidden() && file.isFile();
            }
        };
    }

    public static String getRelativePath(String filePath, String folderPath) {

        if (filePath.startsWith(folderPath)) {
            return filePath.substring(folderPath.length() + 1);
        } else {
            return null;
        }
    }

    /**
     * Move all available files from a directory in landing areas to processing area to process
     * and return a string for the new path. The original directory is perserved.
     * @param dirPath - this is the original file path in the landing folder
     * @return String of new file path in the processing folder
     */
    public static Path moveFilesLandingToProcessing(Path dirPath) {
        String relativePath;
        Path processingPath = null;

        try (DirectoryStream<Path> stream = Files.newDirectoryStream(dirPath, FileUtils.filterOutStartWith())) {
            for (Path landingFilePath : stream) {
                relativePath = FileUtils.getRelativePath(landingFilePath.toString(), AgentConstant.LANDING_FOLDER);
                processingPath = Paths.get(AgentConstant.PROCESSING_FOLDER, relativePath);
                if (!Files.exists(processingPath)) Files.createDirectory(processingPath);
                Files.move(landingFilePath, processingPath.resolve(landingFilePath.getFileName()), REPLACE_EXISTING);
                System.out.println("INFO: File " + landingFilePath.toString() + "is moved to " +
                        processingPath.toString() + " for processing");
            }
            return processingPath;
        } catch (IOException ioe) {
            ioe.printStackTrace();
        }

        return null;
    }

    /**
     * Move files from landing areas to processing area to process and return a string for the new path
     * @param filePath - this is the original file path in the landing folder
     * @return String of new file path in the processed folder
     */
    public static Path moveFileProcessingToProcessed(Path filePath) {
        try {
            String relativePath = FileUtils.getRelativePath(filePath.toString(), AgentConstant.LANDING_FOLDER);
            Path processedPath = Paths.get(AgentConstant.PROCESSED_FOLDER, relativePath);
            Files.move(filePath, processedPath.resolve(filePath.getFileName()), REPLACE_EXISTING);
            System.out.println("INFO: File " + filePath.toString() + "is moved to " + processedPath.toString() +
                    " after processed");
            return processedPath;
        } catch (IOException ioe) {
            ioe.printStackTrace();
        }
        return null;
    }

    public static void renameFile(Path filePath, String postfix) {
        try {
            Path targetPath = Paths.get(filePath.toString() + postfix);
            Files.move(filePath, targetPath, REPLACE_EXISTING);
            System.out.println("INFO: File " + filePath.toString() + "is renamed to " + targetPath.toString());
        } catch (IOException ioe) {
            ioe.printStackTrace();
        }
    }



    public static void main(String [] args)
    {
        String relatvePath = getRelativePath(Paths.get("/home/vagrant/landing/cust_data/a.json").toString(), AgentConstant.LANDING_FOLDER);
        System.out.println("file path is " + Paths.get("/home/vagrant/landing/cust_data/a.json").toString());
        System.out.println("relatvePath is " + relatvePath);
    }

}
