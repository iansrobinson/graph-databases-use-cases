package org.neo4j.graphdatabases.queries.traversals;

import static org.neo4j.graphdb.DynamicRelationshipType.withName;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;

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
import org.neo4j.graphdb.RelationshipType;
import org.neo4j.graphdb.index.Index;
import org.neo4j.graphdb.traversal.BranchState;
import org.neo4j.graphdb.traversal.Evaluation;
import org.neo4j.graphdb.traversal.Evaluator;
import org.neo4j.graphdb.traversal.InitialBranchState;
import org.neo4j.graphdb.traversal.TraversalDescription;
import org.neo4j.helpers.collection.IteratorUtil;
import org.neo4j.kernel.Traversal;

public class ParcelRouteCalculator
{
    private static final PathExpander<Interval> DELIVERY_ROUTE_EXPANDER = new ValidPathExpander(
            withName( "DELIVERY_ROUTE" ),
            Direction.INCOMING
    );

    private static final PathExpander<Interval> CONNECTED_TO_EXPANDER = new ValidPathExpander(
            withName( "CONNECTED_TO" ),
            Direction.BOTH
    );

    private static final TraversalDescription TRAVERSAL = Traversal.description()
            .depthFirst()
            .evaluator( new Evaluator()
            {
                private final RelationshipType DELIVERY_ROUTE = withName( "DELIVERY_ROUTE");

                @Override
                public Evaluation evaluate( Path path )
                {
                    if ( !path.endNode().hasRelationship( DELIVERY_ROUTE, Direction.INCOMING )  )
                    {
                        return Evaluation.INCLUDE_AND_PRUNE;
                    }

                    return Evaluation.INCLUDE_AND_CONTINUE;
                }
            } );

    private static final CostEvaluator<Double> COST_EVALUATOR = CommonEvaluators.doubleCostEvaluator( "cost" );

    private final Index<Node> locationIndex;

    public ParcelRouteCalculator( GraphDatabaseService db )
    {
        this.locationIndex = db.index().forNodes( "location" );
    }

    public Iterable<Node> calculateRoute( String start, String end, Interval interval )
    {
        TraversalDescription deliveryBaseFinder = createDeliveryBaseFinder( interval );

        Collection<Node> upLeg = findRouteToDeliveryBase( start, deliveryBaseFinder );
        Collection<Node> downLeg = findRouteToDeliveryBase( end, deliveryBaseFinder );
        Collection<Node> topRoute = findRouteBetweenDeliveryBases(
                IteratorUtil.last( upLeg ),
                IteratorUtil.last( downLeg ),
                interval );

        return combineRoutes( upLeg, downLeg, topRoute );
    }

    private TraversalDescription createDeliveryBaseFinder( Interval interval )
    {
        return TRAVERSAL.expand( DELIVERY_ROUTE_EXPANDER,
                new InitialBranchState.State<Interval>( interval, interval ) );
    }

    private Set<Node> combineRoutes( Collection<Node> upNodes, Collection<Node> downNodes, Collection<Node> topNodes )
    {
        Set<Node> results = new LinkedHashSet<Node>();
        results.addAll( upNodes );
        results.addAll( topNodes );
        List<Node> downNodesList = new ArrayList<Node>( downNodes );
        Collections.reverse( downNodesList );
        results.addAll( downNodesList );
        return results;
    }

    private Collection<Node> findRouteBetweenDeliveryBases( Node deliveryBase1, Node deliveryBase2, Interval interval )
    {
        PathFinder<WeightedPath> routeBetweenDeliveryBasesFinder = GraphAlgoFactory.dijkstra(
                CONNECTED_TO_EXPANDER,
                new InitialBranchState.State<Interval>( interval, interval ),
                COST_EVALUATOR );
        return IteratorUtil.asCollection(
                routeBetweenDeliveryBasesFinder.findSinglePath( deliveryBase1, deliveryBase2 ).nodes() );
    }

    private Collection<Node> findRouteToDeliveryBase( String startPosition, TraversalDescription deliveryBaseFinder )
    {
        Node startNode = locationIndex.get( "name", startPosition ).getSingle();
        return IteratorUtil.asCollection( deliveryBaseFinder.traverse( startNode ).nodes() );
    }

    private static class ValidPathExpander implements PathExpander<Interval>
    {

        private final RelationshipType relationshipType;
        private final Direction direction;

        private ValidPathExpander( RelationshipType relationshipType, Direction direction )
        {
            this.relationshipType = relationshipType;
            this.direction = direction;
        }

        @Override
        public Iterable<Relationship> expand( Path path, BranchState<Interval> deliveryInterval )
        {
            List<Relationship> results = new ArrayList<Relationship>();
            for ( Relationship r : path.endNode().getRelationships( relationshipType, direction ) )
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
