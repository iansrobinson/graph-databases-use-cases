package org.neo4j.graphdatabases.queries.traversals;

import static org.neo4j.graphdb.DynamicRelationshipType.withName;

import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Path;
import org.neo4j.graphdb.index.Index;
import org.neo4j.graphdb.traversal.Evaluation;
import org.neo4j.graphdb.traversal.Evaluator;
import org.neo4j.graphdb.traversal.TraversalDescription;
import org.neo4j.kernel.Traversal;
import org.neo4j.kernel.Uniqueness;

public class FriendOfAFriendDepth4
{
    private final Index<Node> userIndex;
    private static final TraversalDescription traversalDescription = Traversal.description()
            .breadthFirst()
            .uniqueness( Uniqueness.NODE_GLOBAL )
            .relationships( withName( "FRIEND" ) )
            .evaluator( new Evaluator()
            {
                @Override
                public Evaluation evaluate( Path path )
                {
                    if ( path.length() == 4 )
                    {
                        return Evaluation.INCLUDE_AND_PRUNE;
                    }
                    return Evaluation.EXCLUDE_AND_CONTINUE;

                }
            } );

    public FriendOfAFriendDepth4( GraphDatabaseService db )
    {
        this.userIndex = db.index().forNodes( "user" );
    }

    public Iterable<Node> getFriends( String name )
    {
        return traversalDescription.traverse( userIndex.get( "name", name ).getSingle() ).nodes();
    }
}
