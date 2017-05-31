package com.zycus.java.assignment.CustomDBMap.io;

import java.io.File;
import java.io.IOException;
import java.util.logging.Level;
import java.util.logging.Logger;

import com.zycus.java.assignment.CustomDBMap.Configuration;
import com.zycus.java.assignment.CustomDBMap.Record;

public class BlockingDiskIO extends BaseDiskIO {

    private static final Logger logger = Logger.getLogger(BlockingDiskIO.class.getName());

    public BlockingDiskIO(Configuration config, String file){
        super(config, new File(file));
    }
    public BlockingDiskIO(Configuration config){
        super(config, null);
    }
    public BlockingDiskIO(Configuration config, File f){
        super(config, f);
    }

    @Override
    public Record lookup(long location) {
        return doLookup(location);
    }

    @Override
    public long write(Record r) {
        try {
            return doWrite(r, writer());
        } catch (IOException e) {
            throw newRuntimeException(e);
        }
    }


    private void flush() {
        if(System.currentTimeMillis() - this.getLastFlush() >= getConfig().getFlushInterval()){
            doFlush();
        }
    }

    @Override
    public void vacuum(RecordFilter filter) throws Exception {
        doVacuum(filter);
    }

    @Override
    public void update(Record record) {
       try {
           doUpdate(record);
           flush();
       } catch (IOException e) {
           logger.log(Level.SEVERE, e.getMessage(), e);
       }
    }

    @Override
    public void update(Record...records) {
        try {
            doUpdate(records);
            flush();
        } catch (IOException e) {
            logger.log(Level.SEVERE, e.getMessage(), e);
        }
    }


}

