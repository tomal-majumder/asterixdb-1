package org.apache.asterix.external.input.record.reader.hdfs.shapeFile;


import org.apache.asterix.external.input.record.reader.hdfs.shapeFile.DBFReadSupport.DBFField;
import org.apache.asterix.external.input.record.reader.hdfs.shapeFile.DBFReadSupport.DBFReader;
import org.apache.asterix.external.parser.AbstractDataParser;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapred.FileSplit;
import org.apache.hyracks.data.std.api.IValueReference;

import java.io.DataInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Enumeration;
import java.util.List;
import java.util.zip.ZipEntry;
import java.util.zip.ZipFile;
import java.util.zip.ZipInputStream;

public abstract class AbstractShapeReader<T extends IValueReference> extends AbstractDataParser
        implements RecordReader<Void, T>  {
    //protected final LongWritable m_recordNumber = new LongWritable();
    protected long m_length;
    protected FSDataInputStream m_shpStream;
    protected ShpReader m_shpReader;
    protected FSDataInputStream m_dfbStream;
    protected DBFReader m_dbfReader;
    protected List<Text> m_keys;
    protected long m_recno;


    public AbstractShapeReader(InputSplit inputSplit,
                          JobConf conf, Reporter reporter) throws IOException {
        //System.out.println(inputSplit instanceof FileSplit);
        if (inputSplit instanceof FileSplit) {
            final FileSplit fileSplit = (FileSplit) inputSplit;
            m_length = fileSplit.getLength();
            final Path path = fileSplit.getPath();

            final FileSystem fileSystem = FileSystem.get(conf);
            m_shpStream = fileSystem.open(path);
            String shapePath=path.toString();
            String dbfPath= shapePath.substring(0,shapePath.lastIndexOf('.'))+".dbf";
            m_dfbStream=fileSystem.open(new Path(dbfPath));
            m_shpReader = new ShpReader(m_shpStream);
            m_dbfReader=new DBFReader(m_dfbStream);
            final List<DBFField> fields = m_dbfReader.getFields();
            m_keys = new ArrayList<Text>(fields.size());
            for (final DBFField field : fields)
            {
                m_keys.add(new Text(field.fieldName));
            }



             /*
            ZipInputStream zis = new ZipInputStream(m_shpStream);
            ZipEntry zipEntry = zis.getNextEntry();
            while (zipEntry != null) {
                long s = zipEntry.getSize();
                zipEntry = zis.getNextEntry();
            }

            ZipFile zipFile = new ZipFile(path.toString());
            Enumeration<? extends ZipEntry> entries = zipFile.entries();
            while(entries.hasMoreElements()){
                ZipEntry entry = entries.nextElement();
                if(entry.getName().toLowerCase().endsWith(".shp"))
                    m_shpStream=new DataInputStream(zipFile.getInputStream(entry));
                if(entry.getName().toLowerCase().endsWith(".dbf"))
                    m_dfbStream=new DataInputStream(zipFile.getInputStream(entry));
                //InputStream stream = zipFile.getInputStream(entry);
            }
        }*/

        }
        else
        {
            throw new IOException("Input split is not an instance of FileSplit");
        }

    }

    @Override
    public float getProgress() throws IOException {
        return m_length;
    }

    @Override
    public void close() throws IOException
    {
        if (m_shpStream != null)
        {
            m_shpStream.close();
            m_shpStream = null;
        }
        if (m_dfbStream!= null)
        {
            m_dfbStream.close();
            m_dfbStream= null;
        }
    }
}