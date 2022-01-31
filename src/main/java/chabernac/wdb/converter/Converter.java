package chabernac.wdb.converter;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStream;
import java.io.PrintWriter;
import java.util.Iterator;
import java.util.Map;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class Converter {
    private static final Logger LOGGER      = LogManager.getLogger( Converter.class );

    private final int[]         buffer      = new int[ 5 ];
    private final Database      database    = new Database();

    private int                 p           = 0;
    private int                 recordCount = 0;

    public Converter( File input, File output, boolean dumpBytefile, boolean dumpDebugFile, boolean includeIDColumn,
                      String FIELD_SEPARATOR )
        throws IOException {
        byte[] data = readFile( input );
        if ( dumpBytefile ) {
            File byteFile = new File( output.getParentFile(), "bytes.dat" );
            PrintWriter out = new PrintWriter( new BufferedWriter( new FileWriter( byteFile ) ) );
            for ( int k = 0; k < data.length; k++ ) {
                byte b = data[ k ];
                int v = convertSignedByteToInt( b );
                out.print( "" + k + "   " );
                if ( isNormalChar( v ) ) {
                    out.print( (char) v );
                } else {
                    out.print( "*" );
                }
                out.println( "   " + convertSignedByteToInt( b ) );
            }
            out.close();
        }
        if ( dumpDebugFile ) {
            File byteFile = new File( output.getParentFile(), "bytes.html" );
            PrintWriter out = new PrintWriter( new BufferedWriter( new FileWriter( byteFile ) ) );
            out.println( "<html><head><title>" + input.getPath() + "</title></head><body><h1>" + input.getPath() + "</h1><br>" );
            String tableStart = "<table border=\"1\" bordercolor=\"#EEEEEE\" cellspacing=\"0\" cellpadding=\"0\">";
            StringBuffer characterString = new StringBuffer();
            StringBuffer positions = new StringBuffer();
            StringBuffer characters = new StringBuffer();
            StringBuffer values = new StringBuffer();
            for ( int k = 0; k < data.length; k++ ) {
                byte b = data[ k ];
                int v = convertSignedByteToInt( b );
                positions.append( "<td>" + k + "</td>" );
                if ( isNormalChar( v ) ) {
                    characters.append( "<td bgcolor=\"#AAAAFF\">" + (char) v + "</td>" );
                    characterString.append( (char) v );
                } else {
                    characters.append( "<td bgcolor=\"#DDDDDD\">*</td>" );
                    characterString.append( "*" );
                }
                int value = convertSignedByteToInt( b );
                String colouring = "";
                if ( value == 0 )
                                  colouring = " bgcolor=\"yellow\"";
                if ( value == 15 )
                                   colouring = " bgcolor=\"#88FF33\"";
                values.append( "<td" + colouring + ">" + value + "</td>" );
                if ( k % 100 == 99 || k == data.length - 1 ) {
                    out.println( characterString.toString() );
                    out.println( tableStart );
                    out.println( "<tr><td>Positions</td>" + positions.toString() + "</tr>" );
                    out.println( "<tr><td>Characters</td>" + characters.toString() + "</tr>" );
                    out.println( "<tr><td>Values</td>" + values.toString() + "</tr>" );
                    out.println( "</table><br>" );
                    positions = new StringBuffer();
                    characters = new StringBuffer();
                    values = new StringBuffer();
                    characterString = new StringBuffer();
                }
            }
            out.println( "</body></html>" );
            out.close();
        }
        String currentHeaderTitle = null;
        boolean expectingHeaderEnd = false;
        for ( int i = 0; i < data.length; i++ ) {
            int v = convertSignedByteToInt( data[ i ] );
            if ( isHeaderStart() ) {
                String header = readString( data, i, -1 );
                currentHeaderTitle = header;
                expectingHeaderEnd = true;
            }
            if ( isHeaderEnd() && expectingHeaderEnd ) {
                int headerIndex = data[ i - 4 ];
                this.database.addHeader( headerIndex, currentHeaderTitle );
                LOGGER.trace( "Header (" + headerIndex + "): " + currentHeaderTitle );
                expectingHeaderEnd = false;
            }
            addToBuffer( v );
        }
        int currentRecordNumber = -1;
        int previousFieldNumber = 0;
        for ( int j = 0; j < data.length; j++ ) {
            int v = convertSignedByteToInt( data[ j ] );
            int bytesRead = 0;
            try {
                if ( isFieldBoundary() ) {
                    int fieldLength = -7 + data[ j ];
                    if ( fieldLength > 0 ) {
                        int fieldNumber = data[ j + 2 ];
                        int recordNumber = convertSignedBytePairToInt( data[ j + 4 ], data[ j + 5 ] );
                        String field = readString( data, j + 7, fieldLength );
                        bytesRead += 7;
                        LOGGER.trace( "" + j + ":" + recordNumber + ":" + fieldNumber + ":" + field + ":" + fieldLength );
                        this.database.addField( recordNumber, fieldNumber, field );
                    }
                }
            } catch ( ArrayIndexOutOfBoundsException e ) {
                System.out.println(
                    "Warning: passed end of file reading data - perhaps database file is corrupt or truncated?" );
            }
            addToBuffer( v );
            j += bytesRead;
        }
        LOGGER.trace( "Headers map: " + this.database.getAllHeaders() );
        this.recordCount = 0;
        try {
            PrintWriter out = new PrintWriter( new BufferedWriter( new FileWriter( output ) ) );
            Map headers = this.database.getAllHeaders();
            StringBuilder headerLine = new StringBuilder();
            if ( includeIDColumn ) {
                headerLine.append( "Record ID" );
                headerLine.append( FIELD_SEPARATOR );
            }
            for ( Iterator<Integer> it = headers.keySet().iterator(); it.hasNext(); ) {
                Integer key = it.next();
                String value = (String) headers.get( key );
                headerLine.append( value.trim() );
                headerLine.append( FIELD_SEPARATOR );
            }
            out.println( headerLine.toString() );
            Map records = this.database.getAllRecords();
            for ( Iterator<Integer> iterator1 = records.keySet().iterator(); iterator1.hasNext(); ) {
                Integer key = iterator1.next();
                Map record = (Map) records.get( key );
                this.recordCount++;
                if ( includeIDColumn ) {
                    out.print( key );
                    out.print( FIELD_SEPARATOR );
                }
                int lastFieldNumber = 0;
                for ( Iterator<Integer> it2 = record.keySet().iterator(); it2.hasNext(); ) {
                    Integer fieldNumber = it2.next();
                    String value = (String) record.get( fieldNumber );
                    int commaCount = fieldNumber.intValue() - lastFieldNumber;
                    for ( int cc = 0; cc < commaCount; cc++ )
                        out.print( FIELD_SEPARATOR );
                    out.print( value.trim() );
                    lastFieldNumber = fieldNumber.intValue();
                }
                out.println();
            }
            out.close();
        } catch ( IOException e ) {
            System.out.println( e );
        }
        System.out.println( "Written " + this.recordCount + " records to " + output.getAbsolutePath() );
    }

    private String readString( byte[] data, int i, int length ) {
        if ( data[ i ] != 0 )
                              return "";
        i++;
        StringBuffer sb = new StringBuffer();
        while ( data[ i ] == 0 )
            i++;
        int count = 0;
        while ( i < data.length && data[ i ] > 0 ) {
            sb.append( (char) data[ i ] );
            count++;
            i++;
        }
        if ( count != length && length != -1 )
                                               System.out.println(
                                                   "Warning: string length was " + count + " instead of " + length + " at byte " + ( i - count ) );
        return sb.toString();
    }

    private void printSnippet( byte[] data, int p ) {
        for ( int i = p - 16; i < p + 20; i++ ) {
            int v = data[ i ];
            if ( p == i )
                          System.out.print( "<" );
            System.out.print( v );
            if ( p == i )
                          System.out.print( ">" );
            System.out.print( " " );
        }
        System.out.println();
    }

    private boolean isHeaderStart() {
        String boundary = ",11,0,24,";
        String fromBuffer = getFromBuffer();
        return fromBuffer.endsWith( boundary );
    }

    private boolean isHeaderEnd() {
        String boundary = ",127,15,";
        String fromBuffer = getFromBuffer();
        return fromBuffer.endsWith( boundary );
    }

    private boolean isFieldBoundary() {
        String boundary = ",15,0,";
        String fromBuffer = getFromBuffer();
        return fromBuffer.endsWith( boundary );
    }

    private String getFromBuffer() {
        StringBuffer sb = new StringBuffer();
        sb.append( "," );
        for ( int i = 0; i < this.buffer.length; i++ ) {
            int actual = this.p + i;
            if ( actual >= this.buffer.length )
                                                actual -= this.buffer.length;
            sb.append( this.buffer[ actual ] );
            sb.append( "," );
        }
        return sb.toString();
    }

    private void addToBuffer( int v ) {
        this.buffer[ this.p ] = v;
        this.p++;
        if ( this.p >= this.buffer.length )
                                            this.p = 0;
    }

    private boolean isNormalChar( int v ) {
        if ( v >= 32 && v <= 126 )
                                   return true;
        return false;
    }

    private byte[] readFile( File file ) throws IOException {
        InputStream is = new FileInputStream( file );
        long length = file.length();
        if ( length > 2147483647L )
                                   ;
        byte[] bytes = new byte[ (int) length ];
        int offset = 0;
        int numRead = 0;
        while ( offset < bytes.length && ( numRead = is.read( bytes, offset, bytes.length - offset ) ) >= 0 )
            offset += numRead;
        if ( offset < bytes.length )
                                     throw new IOException( "Could not completely read file " + file.getName() );
        is.close();
        return bytes;
    }

    public int getRecordCount() {
        return this.recordCount;
    }

    private int convertSignedBytePairToInt( byte first, byte second ) {
        int a = first;
        int b = second;
        int result = b * 256;
        if ( a < 0 ) {
            result += a + 256;
        } else {
            result += a;
        }
        return result;
    }

    private int convertSignedByteToInt( byte b ) {
        int v = b;
        if ( v < 0 )
                     v += 128;
        return v;
    }
}
