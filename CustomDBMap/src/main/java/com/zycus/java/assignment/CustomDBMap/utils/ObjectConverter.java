package com.zycus.java.assignment.CustomDBMap.utils;

import java.io.Serializable;

public interface ObjectConverter {
    
    public byte[] serialize(Serializable object) throws Exception;

    public <T> T deserialize(byte[] buffer);

}

