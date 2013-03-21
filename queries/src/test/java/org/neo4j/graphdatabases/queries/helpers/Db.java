package org.neo4j.graphdatabases.queries.helpers;

import java.io.File;
import java.io.IOException;
import java.io.PrintWriter;

import org.neo4j.cypher.javacompat.ExecutionEngine;
import org.neo4j.graphdatabases.queries.testing.IndexParam;
import org.neo4j.graphdatabases.queries.testing.IndexParams;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.NotFoundException;
import org.neo4j.kernel.EmbeddedGraphDatabase;
import org.neo4j.test.AsciiDocGenerator;
import org.neo4j.test.ImpermanentGraphDatabase;
import org.neo4j.visualization.asciidoc.AsciidocHelper;

public final class Db
{
    private Db()
    {
    }

    public static GraphDatabaseService impermanentDb()
    {
        return new ImpermanentGraphDatabase();
    }

    public static GraphDatabaseService tempDb()
    {
        return new EmbeddedGraphDatabase( createTempDatabaseDir().getAbsolutePath() );
    }

    public static GraphDatabaseService createFromCypher( String name, String cypher, IndexParam... indexParams )
    {
        GraphDatabaseService db = Db.impermanentDb();

        return createFromCypher( db, name, cypher, indexParams );
    }

    public static GraphDatabaseService createFromCypher( GraphDatabaseService db, String name, String cypher,
                                                         IndexParam... indexParams )
    {
        ExecutionEngine engine = new ExecutionEngine( db );
        engine.execute( cypher );

        new IndexParams( indexParams ).index( db );

        printGraph( name, db );

        return db;
    }

    private static void printGraph( String name, GraphDatabaseService db )
    {
        try
        {
            printFile( name, AsciidocHelper.createGraphVizDeletingReferenceNode( name, db, "graph" ) );
        }
        catch ( NotFoundException e )
        {
            printFile( name, AsciidocHelper.createGraphViz( name, db, "graph" ) );
        }
    }

    private static void printFile( String fileName, String contents )
    {
        PrintWriter writer = AsciiDocGenerator.getPrintWriter( "../examples/", fileName );
        try
        {
            writer.println( contents );
        }
        finally
        {
            writer.close();
        }
    }

    private static File createTempDatabaseDir()
    {

        File d;
        try
        {
            d = File.createTempFile( "gdb-", "dir" );
            System.out.println( String.format( "Created a new Neo4j database at [%s]", d.getAbsolutePath() ) );
        }
        catch ( IOException e )
        {
            throw new RuntimeException( e );
        }
        if ( !d.delete() )
        {
            throw new RuntimeException( "temp config directory pre-delete failed" );
        }
        if ( !d.mkdirs() )
        {
            throw new RuntimeException( "temp config directory not created" );
        }
        d.deleteOnExit();
        return d;
    }
}
