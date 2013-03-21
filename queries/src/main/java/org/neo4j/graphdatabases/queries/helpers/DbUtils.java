package org.neo4j.graphdatabases.queries.helpers;

import org.neo4j.graphdatabases.queries.testing.TestOutputWriter;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;
import org.neo4j.tooling.GlobalGraphOperations;

public class DbUtils
{
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
