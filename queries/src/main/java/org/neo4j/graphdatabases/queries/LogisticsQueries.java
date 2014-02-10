package org.neo4j.graphdatabases.queries;

import java.util.HashMap;
import java.util.Map;

import org.joda.time.Interval;
import org.neo4j.cypher.javacompat.ExecutionResult;
import org.neo4j.graphdatabases.queries.helpers.ExecutionEngineWrapper;
import org.neo4j.graphdatabases.queries.traversals.ParcelRouteCalculator;
import org.neo4j.graphdatabases.queries.traversals.SimpleParcelRouteCalculator;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;

public class LogisticsQueries
{
    private final ExecutionEngineWrapper executionEngineWrapper;
    private final ParcelRouteCalculator parcelRouteCalculator;
    private final SimpleParcelRouteCalculator simpleParcelRouteCalculator;

    public LogisticsQueries( GraphDatabaseService db, ExecutionEngineWrapper executionEngineWrapper )
    {
        this.executionEngineWrapper = executionEngineWrapper;
        this.parcelRouteCalculator = new ParcelRouteCalculator( db );
        this.simpleParcelRouteCalculator = new SimpleParcelRouteCalculator( db );
    }

    public Iterable<Node> findShortestPathWithParcelRouteCalculator( String start, String end, Interval interval )
    {
        return parcelRouteCalculator.calculateRoute( start, end, interval );
    }

    public Iterable<Node> findShortestPathWithSimpleParcelRouteCalculator( String start, String end, Interval interval )
    {
        return simpleParcelRouteCalculator.calculateRoute( start, end, interval );
    }


    public ExecutionResult findShortestPathWithCypherReduce( String start, String end, Interval interval )
    {
        String query =
                "MATCH (s:Location {name:{startLocation}}),\n" +
                "      (e:Location {name:{endLocation}})\n" +
                "MATCH upLeg = (s)<-[:DELIVERY_ROUTE*1..2]-(db1)\n" +
                "WHERE all(r in relationships(upLeg)\n" +
                "          WHERE r.start_date <= {intervalStart}\n" +
                "          AND r.end_date >= {intervalEnd})\n" +
                "WITH  e, upLeg, db1\n" +
                "MATCH downLeg = (db2)-[:DELIVERY_ROUTE*1..2]->(e)\n" +
                "WHERE all(r in relationships(downLeg)\n" +
                "          WHERE r.start_date <= {intervalStart}\n" +
                "          AND r.end_date >= {intervalEnd})\n" +
                "WITH  db1, db2, upLeg, downLeg\n" +
                "MATCH topRoute = (db1)<-[:CONNECTED_TO]-()-[:CONNECTED_TO*1..3]-(db2)\n" +
                "WHERE all(r in relationships(topRoute)\n" +
                "          WHERE r.start_date <= {intervalStart}\n" +
                "          AND r.end_date >= {intervalEnd})\n" +
                "WITH  upLeg, downLeg, topRoute,\n" +
                "      reduce(weight=0, r in relationships(topRoute) | weight+r.cost) AS score\n" +
                "      ORDER BY score ASC\n" +
                "      LIMIT 1\n" +
                "RETURN (nodes(upLeg) + tail(nodes(topRoute)) + tail(nodes(downLeg))) AS n";

        Map<String, Object> params = new HashMap<String, Object>();
        params.put( "startLocation", start );
        params.put( "endLocation", end );
        params.put( "intervalStart", interval.getStartMillis() );
        params.put( "intervalEnd", interval.getEndMillis() );


        return executionEngineWrapper.execute( query, params );
    }
}
