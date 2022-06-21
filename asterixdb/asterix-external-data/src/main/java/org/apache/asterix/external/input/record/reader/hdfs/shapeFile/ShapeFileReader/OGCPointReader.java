package org.apache.asterix.external.input.record.reader.hdfs.shapeFile.ShapeFileReader;

import com.esri.core.geometry.Point;
import com.esri.core.geometry.SpatialReference;
import com.esri.core.geometry.ogc.OGCPoint;
import org.apache.asterix.external.input.record.reader.hdfs.shapeFile.Writable.OGCPointWritable;
import org.apache.asterix.external.input.record.reader.hdfs.shapeFile.Writable.PointWritable;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.Reporter;

import java.io.IOException;

public class OGCPointReader extends AbstractShpReader<OGCPointWritable>
{
    public OGCPointReader(
            InputSplit inputSplit,
            JobConf conf, Reporter reporter) throws IOException, InterruptedException
    {
        super(inputSplit,conf,reporter);
    }
    @Override
    public boolean next(Void key, OGCPointWritable value) throws IOException {
        boolean hasMore = m_shpReader.hasMore();
        if(!hasMore)
            return false;
        Point p=new Point();
        m_shpReader.queryPoint(p);
        value.setGeometry(new OGCPoint(p, SpatialReference.create(4326)));
        //value=new OGCPointWritable(new OGCPoint(p, SpatialReference.create(4326)));
        return true;
    }

    @Override
    public Void createKey() {
        return null;
    }

    @Override
    public OGCPointWritable createValue() {
        return new OGCPointWritable(new OGCPoint(new Point(),SpatialReference.create(4326)));
    }

    @Override
    public long getPos() throws IOException {
        return m_shpStream.getPos();
    }

}
