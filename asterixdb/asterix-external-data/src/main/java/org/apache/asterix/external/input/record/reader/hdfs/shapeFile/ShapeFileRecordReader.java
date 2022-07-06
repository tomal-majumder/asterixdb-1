package org.apache.asterix.external.input.record.reader.hdfs.shapeFile;

import java.io.IOException;

import org.apache.asterix.external.input.record.ValueReferenceRecord;
import org.apache.asterix.external.input.record.reader.hdfs.AbstractHDFSRecordReader;
import org.apache.asterix.external.input.record.reader.hdfs.parquet.AsterixParquetRuntimeException;
import org.apache.asterix.external.util.HDFSUtils;

import org.apache.asterix.om.types.ARecordType;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.RecordReader;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hyracks.api.exceptions.IWarningCollector;
import org.apache.hyracks.data.std.api.IValueReference;
public class ShapeFileRecordReader<V extends IValueReference> extends AbstractHDFSRecordReader<Void, V> {
    private final IWarningCollector warningCollector;
    private final ARecordType recType;

    public ShapeFileRecordReader(boolean[] read, InputSplit[] inputSplits, String[] readSchedule, String nodeName,
                                 JobConf conf, IWarningCollector warningCollector, ARecordType recType, String requestedFields) {
        super(read, inputSplits, readSchedule, nodeName, new ValueReferenceRecord<>(), conf);
        this.warningCollector = warningCollector;
        this.recType=recType;
        if(inputFormat instanceof OGCGeometryInputFormat){
            ((OGCGeometryInputFormat)inputFormat).setRecordType(recType);
            ((OGCGeometryInputFormat)inputFormat).setRequestedFields(requestedFields);
        }

    }

    @Override
    protected boolean onNextInputSplit() throws IOException {
        return false;
    }

    @Override
    public void close() throws IOException {
        super.close();
        //Issue warning if any was reported
        HDFSUtils.issueWarnings(warningCollector, conf);
    }

    @SuppressWarnings("unchecked")
    @Override
    protected RecordReader<Void, V> getRecordReader(int splitIndex) throws IOException {
        try {
            reader = (RecordReader<Void, V>) inputFormat.getRecordReader(inputSplits[splitIndex], conf, Reporter.NULL);
        } catch (AsterixParquetRuntimeException e) {
            throw e.getHyracksDataException();
        }
        if (value == null) {
            value = reader.createValue();
        }
        return reader;
    }
}
