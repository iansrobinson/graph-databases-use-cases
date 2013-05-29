package org.neo4j.graphdatabases.queries.helpers;

import java.util.HashMap;
import java.util.Map;

import org.neo4j.graphdatabases.queries.testing.TestOutputWriter;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.factory.GraphDatabaseFactory;
import org.neo4j.tooling.GlobalGraphOperations;

import static org.neo4j.helpers.collection.IteratorUtil.count;

public class DbUtils
{
    public static GraphDatabaseService existingDB( String dir )
    {
        GraphDatabaseService db = new GraphDatabaseFactory()
                .newEmbeddedDatabaseBuilder( dir )
                .setConfig( DbUtils.dbConfig() )
                .newGraphDatabase();

        if ( count( GlobalGraphOperations.at( db ).getAllRelationshipTypes().iterator() ) == 0 )
        {
            throw new IllegalStateException( "Performance dataset does not exist. See the Readme for instructions on " +
                    "generating a sample dataset." );
        }

        return db;
    }

    public static Map<String, String> dbConfig()
    {
        Map<String, String> params = new HashMap<String, String>();
        params.put( "dump_configuration", "true" );
        params.put( "cache_type", "gcr" );
        params.put( "allow_store_upgrade", "true" );
        params.put( "online_backup_enabled", "false" );
        return params;
    }

    public static void warmCache( GraphDatabaseService db, TestOutputWriter writer )
    {
        writer.writeln( "BEGIN: Warming cache" );

        for ( Relationship r : GlobalGraphOperations.at( db ).getAllRelationships() )
        {
            r.getPropertyKeys();
            r.getStartNode();
        }
        for ( Node n : GlobalGraphOperations.at( db ).getAllNodes() )
        {
            n.getPropertyKeys();
            for ( Relationship relationship : n.getRelationships() )
            {
                relationship.getStartNode();
            }
        }

        writer.writeln( "\nEND  : Warming cache\n" );
    }

    public static int numberOfItemsInIndex( GraphDatabaseService db, String indexName, String propertyName )
    {
        return db.index().forNodes( indexName ).query( propertyName, "*" ).size();
    }
}
