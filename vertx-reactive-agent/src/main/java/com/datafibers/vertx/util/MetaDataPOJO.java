package com.datafibers.vertx.util;

/**
 * MetaData for client
 */
public class MetaDataPOJO {

    private long fileLength;
    private String createDate;
    private String fileName;
    private String topic;
    private String mode;

    public String toString(){
        StringBuilder sb = new StringBuilder();
        sb.append("************************************");
        sb.append("\nfileLength: ").append(fileLength);
        sb.append("\ncreateDate: ").append(createDate);
        sb.append("\nfileName: ").append(fileName);
        sb.append("\ntopic: ").append(topic);
        sb.append("\nmode: ").append(mode);
        sb.append("\n************************************");
        return sb.toString();
    }

    public long getFileLength() {
        return fileLength;
    }
    public void setFileLength(long fileLength) {
        this.fileLength = fileLength;
    }
    public String getCreateDate() {
        return createDate;
    }
    public void setCreateDate(String createDate) {
        this.createDate = createDate;
    }
    public String getFileName() {
        return fileName;
    }
    public void setFileName(String fileName) {
        this.fileName = fileName;
    }
    public String getTopic() {
        return topic;
    }
    public void setTopic(String topic) {
        this.topic = topic;
    }
    public String getMode() {
        return mode;
    }
    public void setMode(String mode) {
        this.mode = mode;
    }

}
