package org.neo4j.graphdatabases.queries.traversals;

import static org.neo4j.graphdb.DynamicRelationshipType.withName;
import static org.neo4j.graphdb.traversal.Uniqueness.*;

import org.neo4j.graphdb.*;
import org.neo4j.graphdb.index.Index;
import org.neo4j.graphdb.traversal.Evaluation;
import org.neo4j.graphdb.traversal.Evaluator;
import org.neo4j.graphdb.traversal.TraversalDescription;
import org.neo4j.helpers.collection.IteratorUtil;
import org.neo4j.kernel.Traversal;
import org.neo4j.kernel.Uniqueness;

public class FriendOfAFriendDepth4
{
    public static final Label USER = DynamicLabel.label("User");

    private final TraversalDescription traversalDescription;

    private final GraphDatabaseService db;

    public FriendOfAFriendDepth4( GraphDatabaseService db )
    {
        this.db = db;
        traversalDescription = traversalDescription(db);
    }

    private TraversalDescription traversalDescription(GraphDatabaseService db)
    {
        return db.traversalDescription()
                .breadthFirst()
                .uniqueness( NODE_GLOBAL )
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
    }

    public Iterable<Node> getFriends( String name )
    {
        ResourceIterable<Node> users = db.findNodesByLabelAndProperty(USER, "name", name);
        Node startNode = IteratorUtil.single(users);
        return traversalDescription.traverse(startNode).nodes();
    }
}
