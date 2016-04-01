package com.github.rbrugier.esb.serial;

import com.sun.xml.internal.messaging.saaj.util.ByteOutputStream;
import org.apache.kafka.common.serialization.Serializer;

import java.io.IOException;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.util.Map;

public class SerializableSerializer implements Serializer<Serializable> {
    @Override
    public void configure(Map<String, ?> configs, boolean isKey) {

    }

    @Override
    public byte[] serialize(String topic, Serializable data) {
        try (ByteOutputStream bos = new ByteOutputStream();
             ObjectOutputStream outputStream = new ObjectOutputStream(bos)) {
            outputStream.writeObject(data);
            return bos.getBytes();
        } catch (IOException e) {

        }
        return new byte[0];
    }

    @Override
    public void close() {

    }
}
