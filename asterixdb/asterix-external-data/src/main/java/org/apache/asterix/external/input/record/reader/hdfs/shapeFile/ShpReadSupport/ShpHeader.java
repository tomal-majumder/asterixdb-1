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
package org.apache.asterix.external.input.record.reader.hdfs.shapeFile.ShpReadSupport;

import java.io.DataInputStream;
import java.io.IOException;
import java.io.Serializable;

import org.apache.commons.io.EndianUtils;

/**
 */
public class ShpHeader implements Serializable {
    public transient int fileLength;
    public transient int version;
    public transient int shapeType;
    public transient double xmin;
    public transient double ymin;
    public transient double xmax;
    public transient double ymax;
    public transient double zmin;
    public transient double zmax;
    public transient double mmin;
    public transient double mmax;

    public ShpHeader(final DataInputStream dataInputStream) throws IOException {
        final int signature = dataInputStream.readInt();
        if (signature != 9994) {
            throw new IOException("Not a valid shapefile. Expected 9994 as file header !");
        }

        dataInputStream.skip(5 * 4);

        fileLength = dataInputStream.readInt();

        version = EndianUtils.readSwappedInteger(dataInputStream);
        shapeType = EndianUtils.readSwappedInteger(dataInputStream);

        xmin = EndianUtils.readSwappedDouble(dataInputStream);
        ymin = EndianUtils.readSwappedDouble(dataInputStream);
        xmax = EndianUtils.readSwappedDouble(dataInputStream);
        ymax = EndianUtils.readSwappedDouble(dataInputStream);
        zmin = EndianUtils.readSwappedDouble(dataInputStream);
        zmax = EndianUtils.readSwappedDouble(dataInputStream);
        mmin = EndianUtils.readSwappedDouble(dataInputStream);
        mmax = EndianUtils.readSwappedDouble(dataInputStream);
    }

    public boolean isOverlapped(double xmin, double ymin, double xmax, double ymax) {
        return !(xmin > this.xmax) && !(this.xmin > xmax) && !(ymin > this.ymax) && !(this.ymin > ymax);
    }

}
