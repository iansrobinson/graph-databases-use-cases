package org.neo4j.graphdatabases.dataset_builders.traversers;

import static org.neo4j.graphdb.DynamicRelationshipType.withName;

import org.neo4j.graphalgo.GraphAlgoFactory;
import org.neo4j.graphalgo.PathFinder;
import org.neo4j.graphdb.Direction;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Path;
import org.neo4j.graphdb.PathExpander;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.RelationshipExpander;
import org.neo4j.graphdb.RelationshipType;
import org.neo4j.graphdb.traversal.BranchState;
import org.neo4j.graphdb.traversal.Evaluation;
import org.neo4j.graphdb.traversal.Evaluator;
import org.neo4j.graphdb.traversal.TraversalDescription;
import org.neo4j.kernel.Traversal;
import org.neo4j.kernel.Uniqueness;
import org.neo4j.neode.GraphQuery;

public class FindAllColleagues extends GraphQuery
{
    private static final RelationshipType WORKED_ON = withName( "WORKED_ON" );
    private static PathFinder<Path> workedWithPathFinder = GraphAlgoFactory.shortestPath( Traversal.expanderForTypes( withName( "WORKED_WITH" )), 1 );
    private static final TraversalDescription traversal = Traversal.description()
            .depthFirst()
            .uniqueness( Uniqueness.NODE_GLOBAL )
            .expand( new WorkOnPathExpander() )
            .evaluator( new Evaluator()
            {
                @Override
                public Evaluation evaluate( Path path )
                {
                    if ( path.length() == 2 )
                    {
                        if ( workedWithPathFinder.findSinglePath( path.startNode(), path.endNode() ) == null)
                        {
                            return Evaluation.INCLUDE_AND_PRUNE;
                        }
                        else
                        {
                            return Evaluation.EXCLUDE_AND_PRUNE;
                        }
                    }
                    return Evaluation.EXCLUDE_AND_CONTINUE;
                }
            } );

    @Override
    public Iterable<Node> execute( Node node )
    {
        return traversal.traverse( node ).nodes();
    }

    private static class WorkOnPathExpander implements PathExpander<Object>
    {
        @Override
        public Iterable<Relationship> expand( Path path, BranchState<Object> branchState )
        {
            if (path.length() == 0)
            {
                return path.endNode().getRelationships( WORKED_ON, Direction.OUTGOING );
            }
            else
            {
                return path.endNode().getRelationships( WORKED_ON, Direction.INCOMING );
            }
        }

        @Override
        public PathExpander<Object> reverse()
        {
            return null;
        }
    }
}
