package org.neo4j.graphdatabases.queries.testing;

import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;

public class IndexParam
{
    public static IndexParam indexParam( String nodeLabel, String indexName, String propertyName, String indexKey )
    {
        return new IndexParam( nodeLabel, indexName, propertyName, indexKey );
    }

    public static IndexParam indexParam( String nodeLabel, String indexName, String propertyName )
    {
        return new IndexParam( nodeLabel, indexName, propertyName, propertyName );
    }

    public static IndexParam indexParam( String nodeLabel, String propertyName )
    {
        return new IndexParam( nodeLabel, nodeLabel, propertyName, propertyName );
    }

    private final String nodeLabel;
    private final String indexName;
    private final String propertyName;
    private final String indexKey;

    private IndexParam( String nodeLabel, String indexName, String propertyName, String indexKey )
    {
        this.nodeLabel = nodeLabel;
        this.indexName = indexName;
        this.propertyName = propertyName;
        this.indexKey = indexKey;
    }

    public String nodeLabel()
    {
        return nodeLabel;
    }

    public void indexNode( Node node, GraphDatabaseService db )
    {
        db.index().forNodes( indexName ).add( node, indexKey, node.getProperty( propertyName ) );
    }
}
