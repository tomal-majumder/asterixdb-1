package org.apache.asterix.external.input.record.reader.hdfs.shapeFile.DBFReadSupport;

import java.io.Serializable;

/**
 */
public final class DBFType implements Serializable
{

    public final static byte END = 0x1A;
    public final static byte DELETED = 0x2A;

    private DBFType()
    {
    }

}

