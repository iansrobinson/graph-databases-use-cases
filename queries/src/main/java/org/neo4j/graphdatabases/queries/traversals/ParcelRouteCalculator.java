package org.neo4j.graphdatabases.queries.traversals;

import static org.neo4j.graphdb.DynamicRelationshipType.withName;

import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;

import org.joda.time.Interval;
import org.neo4j.graphalgo.CommonEvaluators;
import org.neo4j.graphalgo.CostEvaluator;
import org.neo4j.graphalgo.GraphAlgoFactory;
import org.neo4j.graphalgo.PathFinder;
import org.neo4j.graphalgo.WeightedPath;
import org.neo4j.graphdb.*;
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
    private static final PathExpander<Interval> DELIVERY_ROUTE_EXPANDER = new IntervalPathExpander(
            withName( "DELIVERY_ROUTE" ),
            Direction.INCOMING
    );

    private static final PathExpander<Interval> CONNECTED_TO_EXPANDER = new IntervalPathExpander(
            withName( "CONNECTED_TO" ),
            Direction.BOTH
    );

    private static final TraversalDescription DELIVERY_BASE_FINDER = Traversal.description()
            .depthFirst()
            .evaluator( new Evaluator()
            {
                private final RelationshipType DELIVERY_ROUTE = withName( "DELIVERY_ROUTE");

                @Override
                public Evaluation evaluate( Path path )
                {
                    if ( isDeliveryBase( path ) )
                    {
                        return Evaluation.INCLUDE_AND_PRUNE;
                    }

                    return Evaluation.EXCLUDE_AND_CONTINUE;
                }

                private boolean isDeliveryBase( Path path )
                {
                    return !path.endNode().hasRelationship( DELIVERY_ROUTE, Direction.INCOMING );
                }
            } );

    private static final CostEvaluator<Double> COST_EVALUATOR = CommonEvaluators.doubleCostEvaluator( "cost" );
    public static final Label LOCATION = DynamicLabel.label("Location");
    private GraphDatabaseService db;

    public ParcelRouteCalculator( GraphDatabaseService db )
    {
        this.db = db;
    }

    public Iterable<Node> calculateRoute( String start, String end, Interval interval )
    {
        try ( Transaction tx = db.beginTx() )
        {
            TraversalDescription deliveryBaseFinder = createDeliveryBaseFinder( interval );

            Path upLeg = findRouteToDeliveryBase( start, deliveryBaseFinder );
            Path downLeg = findRouteToDeliveryBase( end, deliveryBaseFinder );

            Path topRoute = findRouteBetweenDeliveryBases(
                    upLeg.endNode(),
                    downLeg.endNode(),
                    interval );

            Set<Node> routes = combineRoutes(upLeg, downLeg, topRoute);
            tx.success();
            return routes;
        }
    }

    private TraversalDescription createDeliveryBaseFinder( Interval interval )
    {
        return DELIVERY_BASE_FINDER.expand( DELIVERY_ROUTE_EXPANDER,
                new InitialBranchState.State<Interval>( interval, interval ) );
    }

    private Set<Node> combineRoutes( Path upLeg, Path downLeg, Path topRoute )
    {
        LinkedHashSet<Node> results = new LinkedHashSet<Node>();
        results.addAll( IteratorUtil.asCollection( upLeg.nodes() ));
        results.addAll( IteratorUtil.asCollection( topRoute.nodes() ));
        results.addAll( IteratorUtil.asCollection( downLeg.reverseNodes() ));
        return results;
    }

    private Path findRouteBetweenDeliveryBases( Node deliveryBase1, Node deliveryBase2, Interval interval )
    {
        PathFinder<WeightedPath> routeBetweenDeliveryBasesFinder = GraphAlgoFactory.dijkstra(
                CONNECTED_TO_EXPANDER,
                new InitialBranchState.State<Interval>( interval, interval ),
                COST_EVALUATOR );
        return routeBetweenDeliveryBasesFinder.findSinglePath( deliveryBase1, deliveryBase2 );
    }

    private Path findRouteToDeliveryBase( String startPosition, TraversalDescription deliveryBaseFinder )
    {
        Node startNode = IteratorUtil.single(db.findNodesByLabelAndProperty(LOCATION, "name", startPosition));
        return deliveryBaseFinder.traverse( startNode ).iterator().next();
    }

    private static class IntervalPathExpander implements PathExpander<Interval>
    {

        private final RelationshipType relationshipType;
        private final Direction direction;

        private IntervalPathExpander( RelationshipType relationshipType, Direction direction )
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
