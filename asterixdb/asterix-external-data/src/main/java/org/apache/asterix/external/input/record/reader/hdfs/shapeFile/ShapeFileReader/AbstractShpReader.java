package org.apache.asterix.external.input.record.reader.hdfs.shapeFile.ShapeFileReader;


import org.apache.asterix.external.input.record.reader.hdfs.shapeFile.ShpReader;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapred.FileSplit;
import java.io.IOException;

/**
 */
abstract class AbstractShpReader<T extends Writable>
        implements RecordReader<Void, T> {
    //protected final LongWritable m_recordNumber = new LongWritable();
    protected long m_length;
    protected FSDataInputStream m_shpStream;
    protected ShpReader m_shpReader;
    public AbstractShpReader(InputSplit inputSplit,
                             JobConf conf, Reporter reporter) throws IOException {
        System.out.println(inputSplit instanceof FileSplit);
        if (inputSplit instanceof FileSplit)
        {
            final FileSplit fileSplit = (FileSplit) inputSplit;
            m_length = fileSplit.getLength();
            final Path path = fileSplit.getPath();
            final FileSystem fileSystem = FileSystem.get(conf);
            m_shpStream = fileSystem.open(path);
            m_shpReader = new ShpReader(m_shpStream);
        }
        else
        {
            throw new IOException("Input split is not an instance of FileSplit");
        }

    }

    @Override
    public float getProgress() throws IOException {
        return m_shpStream.getPos() / m_length;
    }

    @Override
    public void close() throws IOException
    {
        if (m_shpStream != null)
        {
            m_shpStream.close();
            m_shpStream = null;
        }
    }
}