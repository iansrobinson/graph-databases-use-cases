package org.neo4j.graphdatabases.queries.helpers;

import java.util.HashMap;
import java.util.Map;

import org.neo4j.graphdatabases.queries.testing.TestOutputWriter;
import org.neo4j.graphdb.*;
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

        if ( countRelTypes(db) == 0 )
        {
            throw new IllegalStateException( "Performance dataset does not exist. See the Readme for instructions on " +
                    "generating a sample dataset." );
        }

        return db;
    }

    public static int countRelTypes(GraphDatabaseService db) {
        try ( Transaction tx = db.beginTx() )
        {
            int count = count(GlobalGraphOperations.at(db).getAllRelationshipTypes());
            tx.success();
            return count;
        }
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

        try ( Transaction tx = db.beginTx() )
        {
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
            tx.success();
        }
        writer.writeln( "\nEND  : Warming cache\n" );
    }

    public static int numberOfItemsWithLabel(GraphDatabaseService db, String labelName)
    {
        try ( Transaction tx = db.beginTx() )
        {
            GlobalGraphOperations ops = GlobalGraphOperations.at(db);
            int count = count(ops.getAllNodesWithLabel(DynamicLabel.label(labelName)));
            tx.success();
            return count;
        }
    }
}
