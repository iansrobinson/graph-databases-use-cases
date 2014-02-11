package org.neo4j.graphdatabases.queries.traversals;

import static org.neo4j.graphdb.DynamicRelationshipType.withName;

import org.neo4j.graphdatabases.queries.helpers.IndexNodeByOtherNodeIndexer;
import org.neo4j.graphdb.*;
import org.neo4j.graphdb.traversal.Evaluation;
import org.neo4j.graphdb.traversal.Evaluator;
import org.neo4j.graphdb.traversal.TraversalDescription;
import org.neo4j.kernel.Traversal;
import org.neo4j.tooling.GlobalGraphOperations;

public class IndexResources
{
    private final GraphDatabaseService db;
    private final TraversalDescription traversalDescription = Traversal.description()
            .breadthFirst()
            .relationships( withName( "WORKS_FOR" ), Direction.INCOMING )
            .relationships( withName( "HAS_ACCOUNT" ), Direction.OUTGOING )
            .evaluator( new Evaluator()
            {
                @Override
                public Evaluation evaluate( Path path )
                {
                    if ( path.endNode().equals( path.startNode() ) )
                    {
                        return Evaluation.EXCLUDE_AND_CONTINUE;
                    }
                    return Evaluation.INCLUDE_AND_CONTINUE;
                }
            } );

    public IndexResources( GraphDatabaseService db )
    {
        this.db = db;
    }

    public void execute()
    {
        IndexNodeByOtherNodeIndexer.GraphTraversal traversal1 = new IndexNodeByOtherNodeIndexer.GraphTraversal()
        {
            @Override
            public Iterable<Node> execute( Node startNode )
            {
                return GlobalGraphOperations.at(db).getAllNodesWithLabel(DynamicLabel.label("company"));
            }
        };

        IndexNodeByOtherNodeIndexer.GraphTraversal traversal2 = new IndexNodeByOtherNodeIndexer.GraphTraversal()
        {
            @Override
            public Iterable<Node> execute( Node startNode )
            {
                return traversalDescription.traverse( startNode ).nodes();
            }
        };

        IndexNodeByOtherNodeIndexer indexer = new IndexNodeByOtherNodeIndexer( traversal1, traversal2, "company", "resourceName", "name" );
        indexer.execute( db, null, 1000 );
    }
}
