package com.zycus.java.assignment.CustomDBMap;

import java.io.File;

public class Configuration {
    private int flushInterval;
    private File dir;
    private int number;
    private int readerPoolSize = 3;
    private boolean useNonBlockingReader = true;

    public Configuration() {
    }

    public Configuration(Configuration cfg) {
        this.flushInterval = cfg.getFlushInterval();
        this.dir = new File(cfg.getDataDir().getAbsolutePath());
        this.number = cfg.getNumber();
        this.readerPoolSize = cfg.getReaderPoolSize();
        this.useNonBlockingReader = cfg.getUseNonBlockingReader();
    }

    public Configuration setFlushInterval(int interval) {
        this.flushInterval = interval;
        return this;
    }

    public int getFlushInterval() {
        return flushInterval;
    }

    public Configuration setDataDir(File dir) {
        this.dir = dir;
        return this;
    }

    public File getDataDir() {
        return dir;
    }

    public Configuration setNumber(int number) {
        this.number = number;
        return this;
    }

    public int getNumber() {
        return number;
    }

    public String getDataFileName(String extension){
        return getDataDir().getAbsolutePath() + File.separator + getNumber() + "." + extension;
    }

    public int getReaderPoolSize() {
        return readerPoolSize;
    }

    public boolean getUseNonBlockingReader() {
        return useNonBlockingReader;
    }

    public Configuration setUseNonBlockingReader(boolean useNonBlockingReader) {
        this.useNonBlockingReader = useNonBlockingReader;
        return this;
    }
}

