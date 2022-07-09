/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.asterix.external.input.record.reader.hdfs.shapeFile;

// import com.esri.io.PointWritable;
// import java.util.*;
import java.io.IOException;
import java.util.Arrays;
import java.util.List;

import org.apache.asterix.builders.RecordBuilder;
import org.apache.asterix.external.api.IDataParser;
import org.apache.asterix.external.input.record.reader.hdfs.shapeFile.DBFReadSupport.DBFField;
import org.apache.asterix.external.input.record.reader.hdfs.shapeFile.DBFReadSupport.DBFType;
import org.apache.asterix.formats.nontagged.SerializerDeserializerProvider;
import org.apache.asterix.om.base.ABoolean;
import org.apache.asterix.om.base.AGeometry;
import org.apache.asterix.om.base.AMutableString;
import org.apache.asterix.om.base.ANull;
import org.apache.asterix.om.base.temporal.GregorianCalendarSystem;
import org.apache.asterix.om.types.ARecordType;
import org.apache.asterix.om.types.BuiltinType;
import org.apache.asterix.om.types.IAType;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.RecordReader;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hyracks.api.dataflow.value.ISerializerDeserializer;
import org.apache.hyracks.data.std.primitive.VoidPointable;
import org.apache.hyracks.data.std.util.ArrayBackedValueStorage;

// import com.esri.io.PointWritable;
import com.esri.core.geometry.Polygon;
import com.esri.core.geometry.SpatialReference;
// import com.esri.core.geometry.ogc.*;
import com.esri.core.geometry.ogc.OGCGeometry;
import com.esri.core.geometry.ogc.OGCMultiLineString;
import com.esri.core.geometry.ogc.OGCMultiPoint;
import com.esri.core.geometry.ogc.OGCMultiPolygon;
import com.esri.core.geometry.ogc.OGCPoint;
import com.esri.core.geometry.ogc.OGCPolygon;

/**
 */
public class OGCGeometryInputFormat extends AbstractShpInputFormat<VoidPointable> {
    private ARecordType recordType;
    private String requestedFields;
    private String filterMBRInfo;

    @Override
    public RecordReader<Void, VoidPointable> getRecordReader(InputSplit inputSplit, JobConf conf, Reporter reporter)
            throws IOException {
        try {
            return new ShapeFileReader(inputSplit, conf, reporter, recordType, requestedFields, filterMBRInfo);

        } catch (InterruptedException e) {
            e.printStackTrace();
        }

        return null;
    }

    public void setRecordType(ARecordType type) {
        this.recordType = type;
    }

    public void setRequestedFields(String requestedFields) {
        this.requestedFields = requestedFields;
    }

    public void setFilterMBRInfo(String filterMBRInfo) {
        this.filterMBRInfo = filterMBRInfo;
    }

    private static final class ShapeFileReader extends AbstractShapeReader<VoidPointable> {
        private final RecordBuilder builder;
        private final ARecordType recordType;
        private final boolean readGeometryField;
        private final boolean readDBFFields;
        private final String filterMBRInfo;

        private double filterXmin;
        private double filterYMin;
        private double filterXmax;
        private double filterYmax;

        public ShapeFileReader(InputSplit inputSplit, JobConf conf, Reporter reporter, ARecordType recordType,
                String requestedFields, String filterMBRInfo) throws IOException, InterruptedException {
            super(inputSplit, conf, reporter, filterMBRInfo);
            this.recordType = recordType;
            builder = new RecordBuilder();
            this.filterMBRInfo = filterMBRInfo;
            if (requestedFields == null) {
                readGeometryField = true;
                readDBFFields = true;
            } else {
                String[] fields = requestedFields.split(",");

                if (requestedFields.isEmpty()) {
                    readGeometryField = true;
                    readDBFFields = true;
                } else {
                    readGeometryField = Arrays.asList(fields).contains("g");
                    if (readGeometryField && fields.length > 1) {
                        readDBFFields = true;
                    } else
                        readDBFFields = !readGeometryField;
                }
            }

        }

        @Override
        public boolean next(Void key, VoidPointable value) throws IOException {
            boolean hasMore = m_shpReader.hasMore();
            if (!hasMore)
                return false;
            //m_shpReader.queryPoint();
            builder.init();
            builder.reset(this.recordType);
            int fieldIndex;

            if (readGeometryField) {
                OGCGeometry geometry = null;
                boolean hasReadFully = true;
                m_shpReader.readRecordHeader();
                switch (m_shpReader.shapeType) {
                    case 1:
                    case 11: //PointZ
                    case 21: //PointM
                        geometry = new OGCPoint(m_shpReader.readNewPoint(), SpatialReference.create(4326));
                        break;
                    case 3: //PolyLine
                    case 13:
                    case 23:
                        geometry = new OGCMultiLineString(m_shpReader.readNewPolyline(), SpatialReference.create(4326));
                        break;
                    case 8: //MultiPoint
                    case 18: //MultiPointZ
                    case 28: //MultiPointM

                        geometry = new OGCMultiPoint(m_shpReader.readNewMultiPoint(), SpatialReference.create(4326));
                        break;
                    case 5:
                    case 15:
                    case 25:
                        Polygon p = new Polygon();
                        hasReadFully = m_shpReader.readNewPolygon(p);
                        if (hasReadFully) {
                            if (p.getExteriorRingCount() > 1) {
                                geometry = new OGCMultiPolygon(p, SpatialReference.create(4326));
                            } else
                                geometry = new OGCPolygon(p, SpatialReference.create(4326));

                        }
                        break;

                    default:
                        throw new IllegalStateException("Unexpected value: " + m_shpReader.shapeType);
                }

                if (hasReadFully) {
                    String fieldName = "g";
                    fieldIndex = recordType.getFieldIndex(fieldName);

                    ArrayBackedValueStorage valueBuffer = new ArrayBackedValueStorage();
                    ISerializerDeserializer<AGeometry> gSerde =
                            SerializerDeserializerProvider.INSTANCE.getSerializerDeserializer(BuiltinType.AGEOMETRY);
                    aGeomtry.setValue(geometry);
                    IDataParser.toBytes(aGeomtry, valueBuffer, gSerde);

                    if (fieldIndex < 0) {
                        //field is not defined and the type is open
                        AMutableString str = new AMutableString("g");
                        ArrayBackedValueStorage nameBuffer = new ArrayBackedValueStorage();
                        IDataParser.toBytes(str, nameBuffer, stringSerde);
                        builder.addField(nameBuffer, valueBuffer);

                    } else {
                        final IAType fieldType = recordType.getFieldType(fieldName);
                        if (fieldType.getTypeTag() == aGeomtry.getType().getTypeTag()) {
                            builder.addField(fieldIndex, valueBuffer);
                        } else
                            throw new IllegalStateException("Defined type and Parsed Type do not match");

                    }

                }

            }

            //   Map<String, Object> map=m_dbfReader.readRecordAsMap();

            //........ DBF File reading ............
            if (readDBFFields) {
                final byte dataType = m_dbfReader.nextDataType();
                if (dataType != DBFType.END) {
                    List<DBFField> fields = m_dbfReader.getFields();
                    for (DBFField field : fields) {

                        ArrayBackedValueStorage valueBytes = new ArrayBackedValueStorage();
                        final byte bytes[] = new byte[field.fieldLength];
                        m_dbfReader.m_dataInputStream.readFully(bytes);
                        Object val;
                        IAType type;
                        switch (field.actualType) {
                            case "string":
                                val = new String(bytes).trim();
                                aString.setValue((String) val);
                                IDataParser.toBytes(aString, valueBytes, stringSerde);
                                type = BuiltinType.ASTRING;
                                break;

                            case "date":
                                val = readTimeInMillis(bytes);
                                aDate.setValue((Integer) val);
                                IDataParser.toBytes(aDate, valueBytes, dateSerde);
                                type = BuiltinType.ADATE;
                                break;

                            case "float":
                                val = readFloat(bytes);
                                aFloat.setValue((Float) val);
                                IDataParser.toBytes(aFloat, valueBytes, floatSerde);
                                type = BuiltinType.AFLOAT;
                                break;
                            case "bool":
                                val = readLogical(bytes);
                                IDataParser.toBytes((ABoolean) val, valueBytes, booleanSerde);
                                type = BuiltinType.ABOOLEAN;
                                break;

                            case "short":
                                val = readShort(bytes);
                                aInt16.setValue((Short) val);
                                IDataParser.toBytes(aInt16, valueBytes, int16Serde);
                                type = BuiltinType.AINT16;

                                break;
                            case "int":
                                val = readInteger(bytes);
                                aInt32.setValue((Integer) val);
                                IDataParser.toBytes(aInt32, valueBytes, int32Serde);
                                type = BuiltinType.AINT32;

                                break;
                            case "long":
                                val = readLong(bytes);
                                aInt64.setValue((Long) val);
                                IDataParser.toBytes(aInt64, valueBytes, int64Serde);
                                type = BuiltinType.AINT64;

                                break;
                            case "double":
                                val = readDouble(bytes);
                                aDouble.setValue((Double) val);
                                IDataParser.toBytes(aDouble, valueBytes, doubleSerde);
                                type = BuiltinType.ADOUBLE;
                                break;
                            default:
                                IDataParser.toBytes(ANull.NULL, valueBytes, nullSerde);
                                type = BuiltinType.ANULL;
                                break;
                        }
                        fieldIndex = recordType.getFieldIndex(field.fieldName);
                        if (fieldIndex < 0) {
                            //field is not defined and the type is open
                            AMutableString aString = new AMutableString(field.fieldName);
                            ArrayBackedValueStorage nameBytes = new ArrayBackedValueStorage();
                            IDataParser.toBytes(aString, nameBytes, stringSerde);
                            builder.addField(nameBytes, valueBytes);

                        } else {
                            final IAType fieldType = recordType.getFieldType(field.fieldName);
                            if (fieldType.getTypeTag() == type.getTypeTag() || type == BuiltinType.ANULL) {
                                builder.addField(fieldIndex, valueBytes);
                            } else
                                throw new IllegalStateException("Defined type and Parsed Type do not match");

                        }
                    }

                } else {
                    return false;
                }

            }

            ArrayBackedValueStorage valueContainer = new ArrayBackedValueStorage();
            //valueContainer.reset();
            builder.write(valueContainer.getDataOutput(), true);
            value.set(valueContainer);
            return true;
        }

        private int parseInt(final byte[] bytes, final int from, final int to) {
            int result = 0;
            for (int i = from; i < to && i < bytes.length; i++) {
                result *= 10;
                result += bytes[i] - '0';
            }
            return result;
        }

        private short parseShort(final byte[] bytes, final int from, final int to) {
            short result = 0;
            for (int i = from; i < to && i < bytes.length; i++) {
                result *= 10;
                result += bytes[i] - '0';
            }
            return result;
        }

        private long parseLong(final byte[] bytes, final int from, final int to) {
            long result = 0L;
            for (int i = from; i < to && i < bytes.length; i++) {
                result *= 10L;
                result += bytes[i] - '0';
            }
            return result;
        }

        private int trimSpaces(final byte[] bytes) {
            int i = 0, l = bytes.length;
            while (i < l) {
                if (bytes[i] != ' ') {
                    break;
                }
                i++;
            }
            return i;
        }

        private int readTimeInMillis(final byte[] bytes) throws IOException {
            int year = parseInt(bytes, 0, 4);
            int month = parseInt(bytes, 4, 6);
            int day = parseInt(bytes, 6, 8);
            long chronon = GregorianCalendarSystem.getInstance().getChronon(year, month, day, 0, 0, 0, 0);
            return GregorianCalendarSystem.getInstance().getChrononInDays(chronon);
        }

        private ABoolean readLogical(final byte[] bytes) throws IOException {
            switch (bytes[0]) {
                case 'Y':
                case 'y':
                case 'T':
                case 't':
                    return ABoolean.TRUE;
                default:
                    return ABoolean.FALSE;
            }

        }

        private short readShort(final byte[] bytes) throws IOException {
            final int index = trimSpaces(bytes);
            final int length = bytes.length - index;
            if (length == 0 || bytes[index] == '?') {
                return 0;
            }
            return parseShort(bytes, index, bytes.length);
        }

        private int readInteger(final byte[] bytes) throws IOException {
            final int index = trimSpaces(bytes);
            final int length = bytes.length - index;
            if (length == 0 || bytes[index] == '?') {
                return 0;
            }
            return parseInt(bytes, index, bytes.length);
        }

        private long readLong(final byte[] bytes) throws IOException {
            final int index = trimSpaces(bytes);
            final int length = bytes.length - index;
            if (length == 0 || bytes[index] == '?') {
                return 0L;
            }
            return parseLong(bytes, index, bytes.length);
        }

        private float readFloat(final byte[] bytes) throws IOException {
            final int index = trimSpaces(bytes);
            final int length = bytes.length - index;
            if (length == 0 || bytes[index] == '?') {
                return 0.0F;
            }
            // TODO - inline float reader
            return Float.parseFloat(new String(bytes, index, length));
        }

        private double readDouble(final byte[] bytes) throws IOException {
            final int index = trimSpaces(bytes);
            final int length = bytes.length - index;
            if (length == 0 || bytes[index] == '?') {
                return 0.0;
            }
            // TODO - inline double reader
            return Double.parseDouble(new String(bytes, index, length));
        }

        @Override
        public Void createKey() {
            return null;
        }

        @Override
        public VoidPointable createValue() {
            return new VoidPointable();
        }

        @Override
        public long getPos() throws IOException {
            return (long) getProgress();
        }

    }

}
