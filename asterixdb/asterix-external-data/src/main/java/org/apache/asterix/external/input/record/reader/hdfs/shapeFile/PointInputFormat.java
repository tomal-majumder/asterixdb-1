package org.apache.asterix.external.input.record.reader.hdfs.shapeFile;


//import com.esri.io.PointWritable;
import org.apache.asterix.external.input.record.reader.hdfs.shapeFile.Writable.PointWritable;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.RecordReader;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.InputSplit;

import java.io.IOException;

/**
 */
public class PointInputFormat extends AbstractShapeFileFormat<PointWritable>
{

    @Override
    public RecordReader<Void, PointWritable> getRecordReader(InputSplit inputSplit, JobConf conf, Reporter reporter) throws IOException {
        try {
            return new PointReader(inputSplit, conf, reporter);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

        return null;
    }

    private static final class PointReader extends AbstractReader<PointWritable>
    {
        public PointReader(
                InputSplit inputSplit,
                JobConf conf, Reporter reporter) throws IOException, InterruptedException
        {
            super(inputSplit,conf,reporter);
        }

        @Override
        public boolean next(Void key,PointWritable value) throws IOException
        {
            boolean hasMore = m_shpReader.hasMore();
            if(!hasMore)
                return false;
            m_shpReader.queryPoint(value.point);

            return true;
        }

        @Override
        public Void createKey() {
            return null;
        }

        @Override
        public PointWritable createValue() {
            return new PointWritable();
        }

        @Override
        public long getPos() throws IOException {
            return (long) getProgress();
        }

    }


}