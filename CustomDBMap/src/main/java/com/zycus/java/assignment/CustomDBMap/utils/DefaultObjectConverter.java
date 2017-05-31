package com.zycus.java.assignment.CustomDBMap.utils;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;

public class DefaultObjectConverter implements ObjectConverter {

	@Override
	public byte[] serialize(Serializable object) throws Exception {
		ByteArrayOutputStream bout = new ByteArrayOutputStream();
		new ObjectOutputStream(bout).writeObject(object);
		bout.flush();
		return bout.toByteArray();
	}

	@Override
	public <T> T deserialize(byte[] buffer) {
		try {
			ObjectInputStream ios = new ObjectInputStream(new ByteArrayInputStream(buffer));
			return (T) ios.readObject();
		} catch (IOException e) {
			throw new RuntimeException(e);
		} catch (ClassNotFoundException e) {
			throw new RuntimeException(e);
		}
	}
}
