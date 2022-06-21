package org.apache.asterix.external.parser.factory;

import org.apache.asterix.common.exceptions.AsterixException;
import org.apache.asterix.external.api.IRecordDataParser;
import org.apache.asterix.external.api.IRecordDataParserFactory;
import org.apache.asterix.external.parser.PointDataParser;
import org.apache.asterix.external.util.ExternalDataConstants;
import org.apache.asterix.om.types.ARecordType;
import org.apache.hyracks.algebricks.common.exceptions.AlgebricksException;
import org.apache.hyracks.api.context.IHyracksTaskContext;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.data.std.api.IValueReference;

import java.util.Collections;
import java.util.List;
import java.util.Map;

public class PointDataParserFactory implements IRecordDataParserFactory<IValueReference> {
    private static final long serialVersionUID = 5274870143009767516L;

    private static final List<String> PARSER_FORMAT = Collections.singletonList(ExternalDataConstants.FORMAT_SHAPE);

    @Override
    public void configure(Map<String, String> configuration) throws AlgebricksException {
        //nothing
    }

    @Override
    public void setRecordType(ARecordType recordType) throws AsterixException {
        //it always return open type
    }

    @Override
    public void setMetaType(ARecordType metaType) {
        //no meta type
    }

    @Override
    public List<String> getParserFormats() {
        return PARSER_FORMAT;
    }

    @Override
    public IRecordDataParser<IValueReference> createRecordParser(IHyracksTaskContext ctx) throws HyracksDataException {
        return new PointDataParser();
    }

    @Override
    public Class<?> getRecordClass() {
        return IValueReference.class;
    }

}
