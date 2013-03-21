package org.neo4j.graphdatabases.queries.traversals;

import static org.neo4j.graphdb.DynamicRelationshipType.withName;

import java.util.ArrayList;
import java.util.List;

import org.joda.time.Interval;
import org.neo4j.graphalgo.CommonEvaluators;
import org.neo4j.graphalgo.CostEvaluator;
import org.neo4j.graphalgo.GraphAlgoFactory;
import org.neo4j.graphalgo.PathFinder;
import org.neo4j.graphalgo.WeightedPath;
import org.neo4j.graphdb.Direction;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Path;
import org.neo4j.graphdb.PathExpander;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.index.Index;
import org.neo4j.graphdb.traversal.BranchState;
import org.neo4j.graphdb.traversal.InitialBranchState;
import org.neo4j.helpers.collection.IteratorUtil;

public class SimpleParcelRouteCalculator
{
    private static final CostEvaluator<Double> COST_EVALUATOR = CommonEvaluators.doubleCostEvaluator( "cost" );
    private static final PathExpander<Interval> PATH_EXPANDER = new ValidPathExpander();

    private final Index<Node> locationIndex;

    public SimpleParcelRouteCalculator( GraphDatabaseService db )
    {
        this.locationIndex = db.index().forNodes( "location" );
    }

    public Iterable<Node> calculateRoute( String start, String end, Interval interval )
    {
        Node startNode = locationIndex.get( "name", start ).getSingle();
        Node endNode = locationIndex.get( "name", end ).getSingle();

        PathFinder<WeightedPath> routeBetweenDeliveryBasesFinder = GraphAlgoFactory.dijkstra(
                PATH_EXPANDER,
                new InitialBranchState.State<Interval>( interval, interval ),
                COST_EVALUATOR );
        return IteratorUtil.asCollection(
                routeBetweenDeliveryBasesFinder.findSinglePath( startNode, endNode ).nodes() );
    }

    private static class ValidPathExpander implements PathExpander<Interval>
    {
        @Override
        public Iterable<Relationship> expand( Path path, BranchState<Interval> deliveryInterval )
        {
            List<Relationship> results = new ArrayList<Relationship>();
            for ( Relationship r : path.endNode().getRelationships( Direction.BOTH, withName( "CONNECTED_TO" ),
                    withName( "DELIVERY_ROUTE" ) ) )
            {
                Interval relationshipInterval = new Interval(
                        (Long) r.getProperty( "start_date" ),
                        (Long) r.getProperty( "end_date" ) );
                if ( relationshipInterval.contains( deliveryInterval.getState() ) )
                {
                    results.add( r );
                }
            }

            return results;
        }

        @Override
        public PathExpander<Interval> reverse()
        {
            return null;
        }
    }
}
