package org.apache.asterix.external.input.record.reader.hdfs.shapeFile;

import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapred.*;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 */
abstract class AbstractShapeFileFormat <T extends Writable>
        extends FileInputFormat<Void, T>
{

    @Override
    protected FileStatus[] listStatus(final JobConf job) throws IOException
    {
        final FileStatus[] orig = super.listStatus(job);
        final List<FileStatus> list = new ArrayList<FileStatus>(orig.length);
        for (final FileStatus fileStatus : orig)
        {
            final String name = fileStatus.getPath().getName().toLowerCase();
            if (name.endsWith(".shp"))
            {
                list.add(fileStatus);
            }
        }
        final FileStatus[] dest = new FileStatus[list.size()];
        list.toArray(dest);
        return dest;
    }

    @Override
    protected boolean isSplitable(
            FileSystem fs, Path filename)
    {
        return false;
    }

}