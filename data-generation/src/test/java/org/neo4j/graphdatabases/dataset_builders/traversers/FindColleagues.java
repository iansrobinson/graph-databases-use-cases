package org.neo4j.graphdatabases.dataset_builders.traversers;

import static org.neo4j.graphdb.DynamicRelationshipType.withName;

import java.util.ArrayList;
import java.util.List;

import org.joda.time.Duration;
import org.joda.time.Interval;
import org.joda.time.Period;
import org.neo4j.graphdb.Direction;
import org.neo4j.graphdb.DynamicRelationshipType;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Path;
import org.neo4j.graphdb.PathExpander;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.traversal.BranchState;
import org.neo4j.graphdb.traversal.Evaluation;
import org.neo4j.graphdb.traversal.Evaluator;
import org.neo4j.graphdb.traversal.TraversalDescription;
import org.neo4j.kernel.Traversal;
import org.neo4j.kernel.Uniqueness;
import org.neo4j.neode.GraphQuery;

public class FindColleagues extends GraphQuery
{
    private static final TraversalDescription traversal = Traversal.description()
            .depthFirst()
            .uniqueness( Uniqueness.NODE_GLOBAL )
            .relationships( withName( "WORKED_ON" ), Direction.BOTH )
            .expand( new OverlappingWorkedOnRels() )
            .evaluator( new IsColleague() );

    @Override
    public Iterable<Node> execute( Node node )
    {
        return traversal.traverse( node ).nodes();
    }

    private static class IsColleague implements Evaluator
    {
        @Override
        public Evaluation evaluate( Path path )
        {
            if ( path.length() == 2 )
            {
                return Evaluation.INCLUDE_AND_PRUNE;
            }

            return Evaluation.EXCLUDE_AND_CONTINUE;
        }
    }

    private static class OverlappingWorkedOnRels implements PathExpander<Interval>
    {
        private static final DynamicRelationshipType WORKED_ON = withName( "WORKED_ON" );
        private static final Duration ONE_YEAR = new Period( 0, 0, 0, 365, 0, 0, 0, 0 ).toStandardDuration();


        @Override
        public Iterable<Relationship> expand( Path path, BranchState<Interval> branchState )
        {

            if ( path.length() == 1 )
            {
                List<Relationship> rels = new ArrayList<Relationship>();


                Long start_date = (Long) path.lastRelationship().getProperty( "start_date" );
                Long end_date = (Long) path.lastRelationship().getProperty( "end_date" );
                Interval interval = new Interval( start_date, end_date );


                for ( Relationship rel : path.endNode().getRelationships( WORKED_ON, Direction.INCOMING ) )
                {
                    if ( !rel.equals( path.lastRelationship() ) )
                    {
                        Long rel_start_date = (Long) rel.getProperty( "start_date" );
                        Long rel_end_date = (Long) rel.getProperty( "end_date" );
                        Interval rel_interval = new Interval( rel_start_date, rel_end_date );

                        if ( intervalsOverlapByDuration( interval, rel_interval ) )
                        {
                            rels.add( rel );
                        }
                    }
                }

                return rels;
            }

            return path.endNode().getRelationships( WORKED_ON, Direction.OUTGOING );

        }

        @Override
        public PathExpander<Interval> reverse()
        {
            return null;
        }

        private boolean intervalsOverlapByDuration( Interval myInterval, Interval otherInterval )
        {
            return otherInterval.overlaps( myInterval ) &&
                    otherInterval.overlap( myInterval ).toDuration().compareTo( ONE_YEAR ) > 0;
        }
    }

}
