package com.example.model;

import java.io.Serializable;
import java.util.UUID;


public class words implements Serializable {

    private UUID uid;
    private String fileName;
    private String fileType;
    private int primitiveCount=0;
    private int objectCount=0;
    private int functionCount=0;
    private int classCount=0;
    private int interfaceCount=0;

    public words(UUID uid, String fileName ,String fileType,int primitiveCount, int objectCount, int functionCount, int classCount, int interfaceCount) {
        this.uid = uid;
        this.primitiveCount = primitiveCount;
        this.objectCount = objectCount;
        this.functionCount = functionCount;
        this.classCount = classCount;
        this.interfaceCount = interfaceCount;
        this.fileName = fileName;
        this.fileType = fileType;
    }
    public String getFileType() {
        return fileType;
    }

    public void setFileType(String fileType) {
        this.fileType = fileType;
    }
    public String getFileName() {
        return fileName;
    }

    public void setFileName(String fileName) {
        this.fileName = fileName;
    }
    public UUID getUid() {
        return uid;
    }

    public void setUid(UUID uid) {
        this.uid = uid;
    }

    public int getPrimitiveCount() {
        return primitiveCount;
    }

    public void setPrimitiveCount(int primitiveCount) {
        this.primitiveCount = primitiveCount;
    }

    public int getObjectCount() {
        return objectCount;
    }

    public void setObjectCount(int objectCount) {
        this.objectCount = objectCount;
    }

    public int getFunctionCount() {
        return functionCount;
    }

    public void setFunctionCount(int functionCount) {
        this.functionCount = functionCount;
    }

    public int getClassCount() {
        return classCount;
    }

    public void setClassCount(int classCount) {
        this.classCount = classCount;
    }

    public int getInterfaceCount() {
        return interfaceCount;
    }

    public void setInterfaceCount(int interfaceCount) {
        this.interfaceCount = interfaceCount;
    }

}