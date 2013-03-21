package org.neo4j.graphdatabases.queries.helpers;

import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.index.Index;

public class IndexNodeByOtherNodeIndexer
{
    private final GraphTraversal findIndexableNodes;
    private final GraphTraversal findOtherNodesForIndexableNode;
    private final String indexName;
    private final String indexKey;
    private final String nodeToIndexByPropertyName;

    public IndexNodeByOtherNodeIndexer( GraphTraversal findIndexableNodes, GraphTraversal
            findOtherNodesForIndexableNode,
                                        String indexName, String indexKey, String nodeToIndexByPropertyName )

    {
        this.findIndexableNodes = findIndexableNodes;
        this.findOtherNodesForIndexableNode = findOtherNodesForIndexableNode;
        this.indexName = indexName;
        this.indexKey = indexKey;
        this.nodeToIndexByPropertyName = nodeToIndexByPropertyName;
    }

    public void execute( GraphDatabaseService db, Node startNode, int batchSize )
    {
        Index<Node> nodeIndex = db.index().forNodes( indexName );
        Iterable<Node> indexableNodes = findIndexableNodes.execute( startNode );

        Transaction tx = db.beginTx();
        int currentBatchSize = 0;

        try
        {
            for ( Node indexableNode : indexableNodes )
            {
                Iterable<Node> nodesToIndexBy = findOtherNodesForIndexableNode.execute( indexableNode );
                for ( Node node : nodesToIndexBy )
                {
                    nodeIndex.add( indexableNode, indexKey, node.getProperty( nodeToIndexByPropertyName ) );
                    if ( currentBatchSize++ > batchSize )
                    {
                        tx.success();
                        tx.finish();
                        tx = db.beginTx();
                        currentBatchSize = 0;
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

    public interface GraphTraversal
    {
        Iterable<Node> execute( Node startNode );
    }
}
