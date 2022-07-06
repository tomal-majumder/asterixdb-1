package org.apache.asterix.external.parser;


import org.apache.asterix.external.api.IRawRecord;
import org.apache.asterix.external.api.IRecordDataParser;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.data.std.api.IValueReference;

import java.io.DataOutput;
import java.io.IOException;

public class PointDataParser extends AbstractDataParser implements IRecordDataParser<IValueReference> {

    @Override
    public boolean parse(IRawRecord<? extends IValueReference> record, DataOutput out) throws HyracksDataException {
        try {
            out.write(record.getBytes(), 0, record.size());
            return true;
        } catch (IOException e) {
            throw HyracksDataException.create(e);
        }
    }
}
