package org.apache.asterix.external.input.record.reader.hdfs.shapeFile;

import com.esri.core.geometry.*;
import com.esri.core.geometry.MultiPath.*;

//import com.esri.io.PolylineMWritable;
import org.apache.commons.io.EndianUtils;

import java.io.DataInputStream;
import java.io.IOException;
import java.io.Serializable;

/**
 * http://www.esri.com/library/whitepapers/pdfs/shapefile.pdf
 */
public class ShpReader implements Serializable
{
    private transient DataInputStream m_dataInputStream;
    public transient ShpHeader m_shpHeader;

    private transient int m_parts[] = new int[4];

    public transient int recordNumber;
    public transient int contentLength;
    public transient int contentLengthInBytes;
    public transient int shapeType;
    public transient double xmin;
    public transient double ymin;
    public transient double xmax;
    public transient double ymax;
    public transient double mmin;
    public transient double mmax;
    public transient int numParts;
    public transient int numPoints;

    public ShpReader(final DataInputStream dataInputStream) throws IOException
    {
        m_dataInputStream = dataInputStream;
        m_shpHeader = new ShpHeader(dataInputStream);
    }

    public ShpHeader getHeader()
    {
        return m_shpHeader;
    }

    public boolean hasMore() throws IOException
    {
        return m_dataInputStream.available() > 0;
    }

    public void readRecordHeader() throws IOException
    {
        recordNumber = m_dataInputStream.readInt();
        contentLength = m_dataInputStream.readInt();
        contentLengthInBytes = contentLength + contentLength - 4;

        shapeType = EndianUtils.readSwappedInteger(m_dataInputStream);
    }

    public Point readPoint() throws IOException
    {
        return queryPoint(new Point());
    }
    public Point readNewPoint() throws IOException
    {
        Point point=new Point();
        /*
        recordNumber = m_dataInputStream.readInt();
        contentLength = m_dataInputStream.readInt();
        contentLengthInBytes = contentLength + contentLength - 4; //may be 4 minus for reading shapeType
        shapeType = EndianUtils.readSwappedInteger(m_dataInputStream);
        */

        point.setX(EndianUtils.readSwappedDouble(m_dataInputStream));
        point.setY(EndianUtils.readSwappedDouble(m_dataInputStream));
        if(shapeType==21){
            point.setM(EndianUtils.readSwappedDouble(m_dataInputStream));
        }
        else if(shapeType==11){
            point.setZ(EndianUtils.readSwappedDouble(m_dataInputStream));
            point.setM(EndianUtils.readSwappedDouble(m_dataInputStream));

        }
        return point;
    }
    public Polygon readPolygon() throws IOException
    {
        return queryPolygon(new Polygon());
    }
    public Polyline readPolyLine() throws IOException
    {
        return queryPolyLine(new Polyline());
    }
    public MultiPoint readMultiPoint() throws IOException
    {
        return queryMultiPoint(new MultiPoint());
    }

    public Point queryPoint(final Point point) throws IOException
    {
        readRecordHeader();
        point.setX(EndianUtils.readSwappedDouble(m_dataInputStream));
        point.setY(EndianUtils.readSwappedDouble(m_dataInputStream));
        return point;
    }

    public Polygon queryPolygon(final Polygon polygon) throws IOException
    {
        polygon.setEmpty();

        readRecordHeader();

        readShapeHeader();

        for (int i = 0, j = 1; i < numParts; )
        {
            final int count = m_parts[j++] - m_parts[i++];
            for (int c = 0; c < count; c++)
            {
                final double x = EndianUtils.readSwappedDouble(m_dataInputStream);
                final double y = EndianUtils.readSwappedDouble(m_dataInputStream);
                if (c > 0)
                {
                    polygon.lineTo(x, y);

                }
                else
                {
                    polygon.startPath(x, y);
                }
            }
        }

        polygon.closeAllPaths();

        return polygon;
    }

    public Polygon readNewPolygon() throws IOException
    {
        Polygon polygon=new Polygon();
        /*
        recordNumber = m_dataInputStream.readInt();
        contentLength = m_dataInputStream.readInt();
        contentLengthInBytes = contentLength + contentLength - 4; //may be 4 minus for reading shapeType
        shapeType = EndianUtils.readSwappedInteger(m_dataInputStream);
        */
        xmin = EndianUtils.readSwappedDouble(m_dataInputStream);
        ymin = EndianUtils.readSwappedDouble(m_dataInputStream);
        xmax = EndianUtils.readSwappedDouble(m_dataInputStream);
        ymax = EndianUtils.readSwappedDouble(m_dataInputStream);

        numParts = EndianUtils.readSwappedInteger(m_dataInputStream);
        numPoints = EndianUtils.readSwappedInteger(m_dataInputStream);

        if ((numParts + 1) > m_parts.length)
        {
            m_parts = new int[numParts + 1];
        }
        for (int p = 0; p < numParts; p++)
        {
            m_parts[p] = EndianUtils.readSwappedInteger(m_dataInputStream);
        }
        m_parts[numParts] = numPoints;
        Point[] points=new Point[numPoints];
        for(int i=0;i<numPoints;i++){
            final double x = EndianUtils.readSwappedDouble(m_dataInputStream);
            final double y = EndianUtils.readSwappedDouble(m_dataInputStream);
            points[i]=new Point(x,y);
        }
        if(shapeType==15){
            double zMin=EndianUtils.readSwappedDouble(m_dataInputStream);
            double zMax=EndianUtils.readSwappedDouble(m_dataInputStream);
            for(int i=0;i<numPoints;i++){
                final double z = EndianUtils.readSwappedDouble(m_dataInputStream);
                points[i].setZ(z);
            }
            if(contentLengthInBytes> (40+numParts*4+numPoints*16+16+numPoints*8)){
                double mMin=EndianUtils.readSwappedDouble(m_dataInputStream);
                double mMax=EndianUtils.readSwappedDouble(m_dataInputStream);
                for(int i=0;i<numPoints;i++){
                    final double m = EndianUtils.readSwappedDouble(m_dataInputStream);
                    points[i].setM(m);
                }
            }
        }
        if(shapeType==25){  //MultiPointM
            if(contentLengthInBytes> (40+numParts*4+numPoints*16)){
                double mMin=EndianUtils.readSwappedDouble(m_dataInputStream);
                double mMax=EndianUtils.readSwappedDouble(m_dataInputStream);
                for(int i=0;i<numPoints;i++){
                    final double m = EndianUtils.readSwappedDouble(m_dataInputStream);
                    points[i].setM(m);
                }
            }
        }
        for(int i=0;i<numParts;i++){
            int startIndex=m_parts[i];
            int endIndex=m_parts[i+1]-1;
            polygon.startPath(points[startIndex]);
            for(int j=startIndex+1;j<=endIndex;j++) {
                polygon.lineTo(points[j]);
            }
        }
        polygon.closeAllPaths();
        return polygon;
    }

    public Polyline queryPolyLine(Polyline polyline) throws IOException{
        polyline.setEmpty();
        readRecordHeader();

        readShapeHeader();

        for (int i = 0, j = 1; i < numParts; )
        {
            final int count = m_parts[j++] - m_parts[i++];
            for (int c = 0; c < count; c++)
            {
                final double x = EndianUtils.readSwappedDouble(m_dataInputStream);
                final double y = EndianUtils.readSwappedDouble(m_dataInputStream);
                if (c > 0)
                {
                    polyline.lineTo(x, y);
                }
                else
                {
                    polyline.startPath(x, y);
                }
            }
        }

        polyline.closeAllPaths();


        return polyline;
    }
    public Polyline readNewPolyline() throws IOException{
        Polyline polyLine=new Polyline();
        /*
        recordNumber = m_dataInputStream.readInt();
        contentLength = m_dataInputStream.readInt();
        contentLengthInBytes = contentLength + contentLength - 4; //may be 4 minus for reading shapeType
        shapeType = EndianUtils.readSwappedInteger(m_dataInputStream);

         */
        xmin = EndianUtils.readSwappedDouble(m_dataInputStream);
        ymin = EndianUtils.readSwappedDouble(m_dataInputStream);
        xmax = EndianUtils.readSwappedDouble(m_dataInputStream);
        ymax = EndianUtils.readSwappedDouble(m_dataInputStream);

        numParts = EndianUtils.readSwappedInteger(m_dataInputStream);
        numPoints = EndianUtils.readSwappedInteger(m_dataInputStream);

        if ((numParts + 1) > m_parts.length)
        {
            m_parts = new int[numParts + 1];
        }
        for (int p = 0; p < numParts; p++)
        {
            m_parts[p] = EndianUtils.readSwappedInteger(m_dataInputStream);
        }
        m_parts[numParts] = numPoints;
        Point[] points=new Point[numPoints];
        for(int i=0;i<numPoints;i++){
            final double x = EndianUtils.readSwappedDouble(m_dataInputStream);
            final double y = EndianUtils.readSwappedDouble(m_dataInputStream);
            points[i]=new Point(x,y);
        }
        if(shapeType==13){
            double zMin=EndianUtils.readSwappedDouble(m_dataInputStream);
            double zMax=EndianUtils.readSwappedDouble(m_dataInputStream);
            for(int i=0;i<numPoints;i++){
                final double z = EndianUtils.readSwappedDouble(m_dataInputStream);
                points[i].setZ(z);
            }
            if(contentLengthInBytes> (40+numParts*4+numPoints*16+16+numPoints*8)){
                double mMin=EndianUtils.readSwappedDouble(m_dataInputStream);
                double mMax=EndianUtils.readSwappedDouble(m_dataInputStream);
                for(int i=0;i<numPoints;i++){
                    final double m = EndianUtils.readSwappedDouble(m_dataInputStream);
                    points[i].setM(m);
                }
            }
        }
        if(shapeType==23){  //MultiPointM
            if(contentLengthInBytes> (40+numParts*4+numPoints*16)){
                double mMin=EndianUtils.readSwappedDouble(m_dataInputStream);
                double mMax=EndianUtils.readSwappedDouble(m_dataInputStream);
                for(int i=0;i<numPoints;i++){
                    final double m = EndianUtils.readSwappedDouble(m_dataInputStream);
                    points[i].setM(m);
                }
            }
        }
        for(int i=0;i<numParts;i++){
            int startIndex=m_parts[i];
            int endIndex=m_parts[i+1]-1;
            polyLine.startPath(points[startIndex]);
            for(int j=startIndex+1;j<=endIndex;j++) {
                polyLine.lineTo(points[j]);
            }
        }
        polyLine.closeAllPaths();
        return polyLine;
    }

    public MultiPoint queryMultiPoint(MultiPoint multiPoint) throws IOException{

        multiPoint.setEmpty();
        readRecordHeader();
        xmin = EndianUtils.readSwappedDouble(m_dataInputStream);
        ymin = EndianUtils.readSwappedDouble(m_dataInputStream);
        xmax = EndianUtils.readSwappedDouble(m_dataInputStream);
        ymax = EndianUtils.readSwappedDouble(m_dataInputStream);
        numPoints = EndianUtils.readSwappedInteger(m_dataInputStream);
        //Point[] points=new Point[numPoints];

        for(int i=0;i<numPoints;i++){
            final double x = EndianUtils.readSwappedDouble(m_dataInputStream);
            final double y = EndianUtils.readSwappedDouble(m_dataInputStream);
            multiPoint.add(x,y);
        }
        return multiPoint;
    }
    public MultiPoint readNewMultiPoint() throws IOException{
        MultiPoint multiPoint=new MultiPoint();
        /*
        recordNumber = m_dataInputStream.readInt();
        contentLength = m_dataInputStream.readInt();
        contentLengthInBytes = contentLength + contentLength - 4; //may be 4 minus for reading shapeType
        shapeType = EndianUtils.readSwappedInteger(m_dataInputStream);
         */
        xmin = EndianUtils.readSwappedDouble(m_dataInputStream);
        ymin = EndianUtils.readSwappedDouble(m_dataInputStream);
        xmax = EndianUtils.readSwappedDouble(m_dataInputStream);
        ymax = EndianUtils.readSwappedDouble(m_dataInputStream);
        numPoints = EndianUtils.readSwappedInteger(m_dataInputStream);
        Point[] points=new Point[numPoints];
        double[] mValues=new double[numPoints];
        for(int i=0;i<numPoints;i++){
            final double x = EndianUtils.readSwappedDouble(m_dataInputStream);
            final double y = EndianUtils.readSwappedDouble(m_dataInputStream);
            points[i]=new Point(x,y);
        }
        if(shapeType==18){  //MultiPointZ
            double zMin=EndianUtils.readSwappedDouble(m_dataInputStream);
            double zMax=EndianUtils.readSwappedDouble(m_dataInputStream);
            for(int i=0;i<numPoints;i++){
                final double z = EndianUtils.readSwappedDouble(m_dataInputStream);
                points[i].setZ(z);
            }
            if(contentLengthInBytes> (36+numPoints*16+16+numPoints*8)){
                double mMin=EndianUtils.readSwappedDouble(m_dataInputStream);
                double mMax=EndianUtils.readSwappedDouble(m_dataInputStream);
                for(int i=0;i<numPoints;i++){
                    final double m = EndianUtils.readSwappedDouble(m_dataInputStream);
                    points[i].setM(m);
                }
            }
        }
        if(shapeType==28){  //MultiPointM
            if(contentLengthInBytes> (36+numPoints*16)){
                double mMin=EndianUtils.readSwappedDouble(m_dataInputStream);
                double mMax=EndianUtils.readSwappedDouble(m_dataInputStream);
                for(int i=0;i<numPoints;i++){
                    final double m = EndianUtils.readSwappedDouble(m_dataInputStream);
                    points[i].setM(m);
                }
            }
        }
        for(int i=0;i<numPoints;i++){
            multiPoint.add(points[i]);
        }
        return multiPoint;
    }
    private void readShapeHeader() throws IOException
    {
        xmin = EndianUtils.readSwappedDouble(m_dataInputStream);
        ymin = EndianUtils.readSwappedDouble(m_dataInputStream);
        xmax = EndianUtils.readSwappedDouble(m_dataInputStream);
        ymax = EndianUtils.readSwappedDouble(m_dataInputStream);

        numParts = EndianUtils.readSwappedInteger(m_dataInputStream);
        numPoints = EndianUtils.readSwappedInteger(m_dataInputStream);

        if ((numParts + 1) > m_parts.length)
        {
            m_parts = new int[numParts + 1];
        }
        for (int p = 0; p < numParts; p++)
        {
            m_parts[p] = EndianUtils.readSwappedInteger(m_dataInputStream);
        }
        m_parts[numParts] = numPoints;
    }

}