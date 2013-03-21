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
                "START s=node:location(name={startLocation}),\n" +
                "      e=node:location(name={endLocation})\n" +
                "MATCH p1 = s<-[:DELIVERY_ROUTE*1..2]-db1\n" +
                "WHERE ALL(r in relationships(p1)\n" +
                "          WHERE r.start_date <= {intervalStart}\n" +
                "          AND r.end_date >= {intervalEnd})\n" +
                "WITH  e, p1, db1\n" +
                "MATCH p2 = db2-[:DELIVERY_ROUTE*1..2]->e\n" +
                "WHERE ALL(r in relationships(p2)\n" +
                "          WHERE r.start_date <= {intervalStart}\n" +
                "          AND r.end_date >= {intervalEnd})\n" +
                "WITH  db1, db2, p1, p2\n" +
                "MATCH p3 = db1<-[:CONNECTED_TO]-()-[:CONNECTED_TO*1..3]-db2\n" +
                "WHERE ALL(r in relationships(p3)\n" +
                "          WHERE r.start_date <= {intervalStart}\n" +
                "          AND r.end_date >= {intervalEnd})\n" +
                "WITH  p1, p2, p3,\n" +
                "      REDUCE(weight=0, r in relationships(p3) : weight+r.cost) AS score\n" +
                "      ORDER BY score ASC\n" +
                "      LIMIT 1\n" +
                "RETURN (nodes(p1) + tail(nodes(p3)) + tail(nodes(p2))) AS n";

        Map<String, Object> params = new HashMap<String, Object>();
        params.put( "startLocation", start );
        params.put( "endLocation", end );
        params.put( "intervalStart", interval.getStartMillis() );
        params.put( "intervalEnd", interval.getEndMillis() );


        return executionEngineWrapper.execute( query, params );
    }
}
