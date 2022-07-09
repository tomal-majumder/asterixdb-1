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
// import com.esri.io.PointWritable;
import java.io.IOException;

// import com.esri.io.PointWritable;
import org.apache.asterix.external.input.record.reader.hdfs.shapeFile.Writable.PointWritable;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.RecordReader;
import org.apache.hadoop.mapred.Reporter;

/**
 */
public class PointInputFormat extends AbstractShapeFileFormat<PointWritable> {

    @Override
    public RecordReader<Void, PointWritable> getRecordReader(InputSplit inputSplit, JobConf conf, Reporter reporter)
            throws IOException {
        try {
            return new PointReader(inputSplit, conf, reporter);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

        return null;
    }

    private static final class PointReader extends AbstractReader<PointWritable> {
        public PointReader(InputSplit inputSplit, JobConf conf, Reporter reporter)
                throws IOException, InterruptedException {
            super(inputSplit, conf, reporter);
        }

        @Override
        public boolean next(Void key, PointWritable value) throws IOException {
            boolean hasMore = m_shpReader.hasMore();
            if (!hasMore)
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
