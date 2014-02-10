package org.neo4j.graphdatabases.queries.helpers;

import java.io.File;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.HashMap;
import java.util.Map;

import org.neo4j.cypher.javacompat.ExecutionEngine;
import org.neo4j.graphdatabases.queries.testing.IndexParam;
import org.neo4j.graphdatabases.queries.testing.IndexParams;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.NotFoundException;
import org.neo4j.graphdb.factory.GraphDatabaseFactory;
import org.neo4j.graphdb.index.AutoIndexer;
import org.neo4j.test.AsciiDocGenerator;
import org.neo4j.test.TestGraphDatabaseFactory;
import org.neo4j.visualization.asciidoc.AsciidocHelper;

public final class Db
{
    private Db()
    {
    }

    public static GraphDatabaseService impermanentDb()
    {
        Map<String, String> params = new HashMap<String, String>();
        params.put( "online_backup_enabled", "false" );

        return new TestGraphDatabaseFactory().newImpermanentDatabaseBuilder().setConfig( params ).newGraphDatabase();
    }

    public static GraphDatabaseService tempDb()
    {
        return new GraphDatabaseFactory().newEmbeddedDatabase( createTempDatabaseDir().getAbsolutePath() );
    }

    public static GraphDatabaseService createFromCypher( String name, String cypher, IndexParam... indexParams )
    {
        GraphDatabaseService db = Db.impermanentDb();

        return createFromCypher( db, name, cypher, indexParams );
    }

    public static GraphDatabaseService createFromCypherWithAutoIndexing( String name, String cypher,
                                                                         IndexParam... indexParams )
    {
        GraphDatabaseService db = Db.impermanentDb();

        AutoIndexer<Node> nodeAutoIndexer = db.index().getNodeAutoIndexer();
        for ( IndexParam indexParam : indexParams )
        {
            nodeAutoIndexer.startAutoIndexingProperty( indexParam.propertyName() );
        }
        nodeAutoIndexer.setEnabled( true );

        return createFromCypher( db, name, cypher, indexParams );
    }

    public static GraphDatabaseService createFromCypher( GraphDatabaseService db, String name, String cypher,
                                                         IndexParam... indexParams )
    {
        ExecutionEngine engine = new ExecutionEngine( db );
        engine.execute( cypher );

        new IndexParams( indexParams ).index( db);

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
