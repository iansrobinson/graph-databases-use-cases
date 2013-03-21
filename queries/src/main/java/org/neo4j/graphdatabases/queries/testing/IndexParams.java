package org.neo4j.graphdatabases.queries.testing;

import static java.util.Arrays.asList;

import java.util.List;

import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Transaction;
import org.neo4j.tooling.GlobalGraphOperations;

public class IndexParams
{
    private final List<IndexParam> indexParams;

    public IndexParams( IndexParam... params )
    {
        indexParams = asList( params );
    }

    public void index( GraphDatabaseService db )
    {
        Transaction tx = db.beginTx();
        try
        {
            Iterable<Node> allNodes = GlobalGraphOperations.at( db ).getAllNodes();
            for ( Node node : allNodes )
            {
                if ( node.hasProperty( "_label" ) )
                {
                    String nodeLabel = node.getProperty( "_label" ).toString();
                    for ( IndexParam indexParam : indexParams )
                    {
                        if (indexParam.nodeLabel().equals( nodeLabel ))
                        {
                            indexParam.indexNode( node, db );
                        }
                    }
                }
            }
            tx.success();
        }
        finally
        {
            tx.finish();
        }

    }
}
