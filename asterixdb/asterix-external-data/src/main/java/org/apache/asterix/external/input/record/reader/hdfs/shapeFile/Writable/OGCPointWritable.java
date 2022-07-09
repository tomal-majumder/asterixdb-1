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
package org.apache.asterix.external.input.record.reader.hdfs.shapeFile.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import com.esri.core.geometry.Point;
import com.esri.core.geometry.SpatialReference;
import com.esri.core.geometry.ogc.OGCPoint;

public class OGCPointWritable extends OGCGeometryWritable {

    public OGCPointWritable(OGCPoint point) {
        super(point);
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        if (super.geometry instanceof OGCPoint) {
            OGCPoint p = (OGCPoint) geometry;
            dataOutput.writeDouble(p.X());
            dataOutput.writeDouble(p.Y());
        }
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        if (geometry instanceof OGCPoint) {
            Point p = new Point(dataInput.readDouble(), dataInput.readDouble());
            geometry = new OGCPoint(p, SpatialReference.create(4362));
        }
    }
}
