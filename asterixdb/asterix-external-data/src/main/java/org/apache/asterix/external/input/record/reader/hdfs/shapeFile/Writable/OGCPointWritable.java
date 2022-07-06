package org.apache.asterix.external.input.record.reader.hdfs.shapeFile.Writable;

import com.esri.core.geometry.Point;
import com.esri.core.geometry.SpatialReference;
import com.esri.core.geometry.ogc.OGCGeometry;
import com.esri.core.geometry.ogc.OGCPoint;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class OGCPointWritable extends OGCGeometryWritable{

    public OGCPointWritable(OGCPoint point) {
        super(point);
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        if(super.geometry instanceof OGCPoint){
            OGCPoint p=(OGCPoint)geometry;
            dataOutput.writeDouble(p.X());
            dataOutput.writeDouble(p.Y());
        }
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        if(geometry instanceof OGCPoint){
            Point p=new Point(dataInput.readDouble(),dataInput.readDouble());
            geometry= new OGCPoint(p, SpatialReference.create(4362));
        }
    }
}
