package org.neo4j.graphdatabases.queries.helpers;

import org.neo4j.graphdb.*;
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

        Transaction tx = db.beginTx();

        Label label = DynamicLabel.label(indexName);
        Iterable<Node> indexableNodes = findIndexableNodes.execute( startNode );
        int currentBatchSize = 0;

        try
        {
            for ( Node indexableNode : indexableNodes )
            {
                Iterable<Node> nodesToIndexBy = findOtherNodesForIndexableNode.execute( indexableNode );
                for ( Node node : nodesToIndexBy )
                {
                    indexableNode.addLabel(label);
                    indexableNode.setProperty(indexKey, node.getProperty( nodeToIndexByPropertyName ) );
                    if ( currentBatchSize++ > batchSize )
                    {
                        tx.success();
                        tx.close();
                        tx = db.beginTx();
                        currentBatchSize = 0;
                    }
                }
            }
            tx.success();
        }
        finally
        {
            tx.close();
        }

    }

    public interface GraphTraversal
    {
        Iterable<Node> execute( Node startNode );
    }
}
