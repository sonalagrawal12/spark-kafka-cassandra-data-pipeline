package com.example;

import com.example.model.FileContent;
import org.apache.commons.lang3.SerializationUtils;
import org.apache.kafka.common.serialization.Serializer;


import java.io.File;
import java.io.Serializable;
import java.util.Map;

public class FileContentSerializer <T extends Serializable> implements Serializer<T>  {

public FileContentSerializer(){}
@Override
public void configure(Map<String, ?> configs, boolean isKey) {

}

@Override
public byte[] serialize(String topic, T data) {
        return SerializationUtils.serialize(data);
}

@Override
public void close() {
        }

}
