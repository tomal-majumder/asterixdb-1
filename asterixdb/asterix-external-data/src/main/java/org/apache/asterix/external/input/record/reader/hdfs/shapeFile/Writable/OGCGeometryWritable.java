package org.apache.asterix.external.input.record.reader.hdfs.shapeFile.Writable;

import com.esri.core.geometry.Point;
import com.esri.core.geometry.ogc.OGCGeometry;
import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;


/**
 */
public abstract class OGCGeometryWritable implements Writable
{
    public OGCGeometry geometry;

    public OGCGeometryWritable(OGCGeometry geometry)
    {
        this.geometry=geometry;
    }

    public void setGeometry(OGCGeometry geometry){
        this.geometry=geometry;
    }
}
