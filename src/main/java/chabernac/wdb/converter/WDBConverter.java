package chabernac.wdb.converter;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;

public class WDBConverter implements Runnable {
    private final Path searchFolder;

    public WDBConverter( Path searchFolder ) {
        super();
        this.searchFolder = searchFolder;
    }

    public void run() {
        try {
            Files.walk( searchFolder, 10 )
                .filter( path -> path.toString().toLowerCase().endsWith( ".wdb" ) )
                .forEach( path -> convert( path ) );
        } catch ( IOException e ) {
            e.printStackTrace();
        }
    }

    private void convert( Path path ) {
        System.out.println( "Converting '" + path.toString() + "'" );
        String[] fileNameParts = path.getFileName().toString().split( "\\." );
        Path newFile = Path.of( path.getParent().toAbsolutePath().toString(), fileNameParts[ 0 ] + ".csv" );
        try {
            new Converter( path.toFile(), newFile.toFile(), false, false, false, "," );
            System.out.println( "New file created: '" + newFile.toString() + "'" );
        } catch ( IOException e ) {
            e.printStackTrace();
        }
    }

    public static void main( String[] args ) {
        if ( args.length != 1 ) {
            System.out.println( "First argument needs to be a folder path" );
            return;
        }
        Path path = Path.of( args[ 0 ] );

        if ( !Files.exists( path ) ) {
            System.out.println( "The given path '" + path + "' does not exist" );
            return;
        }

        System.out.println( "Running WDB Converter..." );
        new WDBConverter( path ).run();
        System.out.println( "Converter finished" );
    }

}
