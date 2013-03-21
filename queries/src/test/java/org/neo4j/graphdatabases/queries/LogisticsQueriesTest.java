package org.neo4j.graphdatabases.queries;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.neo4j.graphdatabases.queries.helpers.Db.createFromCypher;
import static org.neo4j.graphdatabases.queries.testing.IndexParam.indexParam;

import java.util.Iterator;

import org.joda.time.DateTime;
import org.joda.time.Interval;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestName;
import org.neo4j.cypher.javacompat.ExecutionResult;
import org.neo4j.graphdatabases.queries.helpers.PrintingExecutionEngineWrapper;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;

public class LogisticsQueriesTest
{
    @Rule
    public static TestName name = new TestName();

    private static GraphDatabaseService db;
    private static LogisticsQueries queries;

    private static Interval interval1 = Interval.parse( "2012-10-15T00:00:00.000+01:00/2012-10-22T00:00:00.000+01:00" );
    private static Interval interval2 = Interval.parse( "2012-10-22T00:00:00.000+01:00/2012-10-29T00:00:00.000+01:00" );
    private static Interval interval3 = Interval.parse( "2012-10-29T00:00:00.000+01:00/2012-11-05T00:00:00.000+01:00" );


    @BeforeClass
    public static void init()
    {
        try
        {

            db = createDatabase();
            queries = new LogisticsQueries( db, new PrintingExecutionEngineWrapper( db, "logistics", name ) );
        }
        catch ( Exception e )
        {
            System.out.println( e.getMessage() );
        }
    }

    @AfterClass
    public static void shutdown()
    {
        db.shutdown();
    }

    @Test
    public void parcelRoutingUsingParcelRouteCalculator() throws Exception
    {
        // given
        DateTime startDtm = interval1.getStart().plusDays( 2 );
        Interval queryInterval = new Interval( startDtm, startDtm.plusDays( 1 ) );

        // when
        Iterable<Node> results = queries.findShortestPathWithParcelRouteCalculator( "delivery-area-1",
                "delivery-segment-3",
                queryInterval );


        // then
        Iterator<Node> iterator = results.iterator();


        assertEquals( "delivery-area-1", iterator.next().getProperty( "name" ) );
        assertEquals( "delivery-base-1", iterator.next().getProperty( "name" ) );
        assertEquals( "parcel-centre-1", iterator.next().getProperty( "name" ) );
        assertEquals( "delivery-base-2", iterator.next().getProperty( "name" ) );
        assertEquals( "parcel-centre-2", iterator.next().getProperty( "name" ) );
        assertEquals( "delivery-base-3", iterator.next().getProperty( "name" ) );
        assertEquals( "delivery-area-2", iterator.next().getProperty( "name" ) );
        assertEquals( "delivery-segment-3", iterator.next().getProperty( "name" ) );

        assertFalse( iterator.hasNext() );
    }

    @Test
    public void parcelRoutingUsingParcelRouteCalculatorChoosesShortestRouteBetweenDeliveryBases() throws Exception
    {
        // given
        DateTime startDtm = interval2.getStart().plusDays( 2 );
        Interval queryInterval = new Interval( startDtm, startDtm.plusDays( 1 ) );

        // when
        Iterable<Node> results = queries.findShortestPathWithParcelRouteCalculator( "delivery-area-1",
                "delivery-segment-3",
                queryInterval );


        // then
        Iterator<Node> iterator = results.iterator();


        assertEquals( "delivery-area-1", iterator.next().getProperty( "name" ) );
        assertEquals( "delivery-base-1", iterator.next().getProperty( "name" ) );
        assertEquals( "parcel-centre-2", iterator.next().getProperty( "name" ) );
        assertEquals( "delivery-base-3", iterator.next().getProperty( "name" ) );
        assertEquals( "delivery-area-2", iterator.next().getProperty( "name" ) );
        assertEquals( "delivery-segment-3", iterator.next().getProperty( "name" ) );

        assertFalse( iterator.hasNext() );
    }

    @Test
    public void parcelRoutingUsingParcelRouteCalculatorRespectsIntervals() throws Exception
    {
        // given
        DateTime startDtm = interval3.getStart().plusDays( 2 );
        Interval queryInterval = new Interval( startDtm, startDtm.plusDays( 1 ) );

        // when
        Iterable<Node> results = queries.findShortestPathWithParcelRouteCalculator( "delivery-area-1",
                "delivery-segment-3",
                queryInterval );


        // then
        Iterator<Node> iterator = results.iterator();


        assertEquals( "delivery-area-1", iterator.next().getProperty( "name" ) );
        assertEquals( "delivery-base-1", iterator.next().getProperty( "name" ) );
        assertEquals( "parcel-centre-1", iterator.next().getProperty( "name" ) );
        assertEquals( "delivery-base-3", iterator.next().getProperty( "name" ) );
        assertEquals( "delivery-area-3", iterator.next().getProperty( "name" ) );
        assertEquals( "delivery-segment-3", iterator.next().getProperty( "name" ) );

        assertFalse( iterator.hasNext() );
    }

    @Test
    public void parcelRoutingUsingSimpleParcelRouteCalculatorRespectsIntervals() throws Exception
    {
        // given
        DateTime startDtm = interval3.getStart().plusDays( 2 );
        Interval queryInterval = new Interval( startDtm, startDtm.plusDays( 1 ) );

        // when
        Iterable<Node> results = queries.findShortestPathWithSimpleParcelRouteCalculator( "delivery-area-1",
                "delivery-segment-3",
                queryInterval );


        // then
        Iterator<Node> iterator = results.iterator();


        assertEquals( "delivery-area-1", iterator.next().getProperty( "name" ) );
        assertEquals( "delivery-base-1", iterator.next().getProperty( "name" ) );
        assertEquals( "parcel-centre-1", iterator.next().getProperty( "name" ) );
        assertEquals( "delivery-base-3", iterator.next().getProperty( "name" ) );
        assertEquals( "delivery-area-3", iterator.next().getProperty( "name" ) );
        assertEquals( "delivery-segment-3", iterator.next().getProperty( "name" ) );

        assertFalse( iterator.hasNext() );
    }

    @Test
    public void withinScopeOfSingleParcelCentreParcelRouteCalculator() throws Exception
    {
        // given
        DateTime startDtm = interval2.getStart().plusDays( 2 );
        Interval queryInterval = new Interval( startDtm, startDtm.plusDays( 1 ) );

        // when
        Iterable<Node> results = queries.findShortestPathWithParcelRouteCalculator( "delivery-area-1",
                "delivery-segment-8",
                queryInterval );


        // then
        Iterator<Node> iterator = results.iterator();


        assertEquals( "delivery-area-1", iterator.next().getProperty( "name" ) );
        assertEquals( "delivery-base-1", iterator.next().getProperty( "name" ) );
        assertEquals( "parcel-centre-1", iterator.next().getProperty( "name" ) );
        assertEquals( "delivery-base-2", iterator.next().getProperty( "name" ) );
        assertEquals( "delivery-area-4", iterator.next().getProperty( "name" ) );
        assertEquals( "delivery-segment-8", iterator.next().getProperty( "name" ) );

        assertFalse( iterator.hasNext() );
    }

    @Test
    public void parcelRoutingUsingCypherReduce() throws Exception
    {
        // given
        DateTime startDtm = interval1.getStart().plusDays( 2 );
        Interval queryInterval = new Interval( startDtm, startDtm.plusDays( 1 ) );

        // when
        ExecutionResult result = queries.findShortestPathWithCypherReduce( "delivery-area-1", "delivery-segment-3",
                queryInterval );


        // then
        Iterator<Iterable<Node>> rows = result.columnAs( "n" );
        Iterator<Node> nodes = rows.next().iterator();


        assertEquals( "delivery-area-1", nodes.next().getProperty( "name" ) );
        assertEquals( "delivery-base-1", nodes.next().getProperty( "name" ) );
        assertEquals( "parcel-centre-1", nodes.next().getProperty( "name" ) );
        assertEquals( "delivery-base-2", nodes.next().getProperty( "name" ) );
        assertEquals( "parcel-centre-2", nodes.next().getProperty( "name" ) );
        assertEquals( "delivery-base-3", nodes.next().getProperty( "name" ) );
        assertEquals( "delivery-area-2", nodes.next().getProperty( "name" ) );
        assertEquals( "delivery-segment-3", nodes.next().getProperty( "name" ) );

        assertFalse( nodes.hasNext() );
    }

    @Test
    public void parcelRoutingUsingCypherReduceChoosesShortestRouteBetweenDeliveryBases() throws Exception
    {
        // given
        DateTime startDtm = interval2.getStart().plusDays( 2 );
        Interval queryInterval = new Interval( startDtm, startDtm.plusDays( 1 ) );

        // when
        ExecutionResult result = queries.findShortestPathWithCypherReduce( "delivery-area-1", "delivery-segment-3",
                queryInterval );


        // then
        Iterator<Iterable<Node>> rows = result.columnAs( "n" );
        Iterator<Node> nodes = rows.next().iterator();

        assertEquals( "delivery-area-1", nodes.next().getProperty( "name" ) );
        assertEquals( "delivery-base-1", nodes.next().getProperty( "name" ) );
        assertEquals( "parcel-centre-2", nodes.next().getProperty( "name" ) );
        assertEquals( "delivery-base-3", nodes.next().getProperty( "name" ) );
        assertEquals( "delivery-area-2", nodes.next().getProperty( "name" ) );
        assertEquals( "delivery-segment-3", nodes.next().getProperty( "name" ) );

        assertFalse( nodes.hasNext() );
    }

    @Test
    public void parcelRoutingUsingCypherReduceRespectsIntervals() throws Exception
    {
        // given
        DateTime startDtm = interval3.getStart().plusDays( 2 );
        Interval queryInterval = new Interval( startDtm, startDtm.plusDays( 1 ) );

        // when
        ExecutionResult result = queries.findShortestPathWithCypherReduce( "delivery-area-1", "delivery-segment-3",
                queryInterval );


        // then
        Iterator<Iterable<Node>> rows = result.columnAs( "n" );
        Iterator<Node> nodes = rows.next().iterator();

        assertEquals( "delivery-area-1", nodes.next().getProperty( "name" ) );
        assertEquals( "delivery-base-1", nodes.next().getProperty( "name" ) );
        assertEquals( "parcel-centre-1", nodes.next().getProperty( "name" ) );
        assertEquals( "delivery-base-3", nodes.next().getProperty( "name" ) );
        assertEquals( "delivery-area-3", nodes.next().getProperty( "name" ) );
        assertEquals( "delivery-segment-3", nodes.next().getProperty( "name" ) );

        assertFalse( nodes.hasNext() );
    }

    @Test
    public void withinScopeOfSingleParcelCentreCypher() throws Exception
    {
        // given
        DateTime startDtm = interval2.getStart().plusDays( 2 );
        Interval queryInterval = new Interval( startDtm, startDtm.plusDays( 1 ) );

        // when
        ExecutionResult result = queries.findShortestPathWithCypherReduce( "delivery-area-1", "delivery-segment-8",
                queryInterval );


        // then
        Iterator<Iterable<Node>> rows = result.columnAs( "n" );
        Iterator<Node> nodes = rows.next().iterator();

        assertEquals( "delivery-area-1", nodes.next().getProperty( "name" ) );
        assertEquals( "delivery-base-1", nodes.next().getProperty( "name" ) );
        assertEquals( "parcel-centre-1", nodes.next().getProperty( "name" ) );
        assertEquals( "delivery-base-2", nodes.next().getProperty( "name" ) );
        assertEquals( "delivery-area-4", nodes.next().getProperty( "name" ) );
        assertEquals( "delivery-segment-8", nodes.next().getProperty( "name" ) );


        assertFalse( nodes.hasNext() );
    }

    private static GraphDatabaseService createDatabase()
    {
        String cypher = "CREATE\n" +
                "pc1 = {name:'parcel-centre-1', _label:'parcel-centre'},\n" +
                "pc2 = {name:'parcel-centre-2', _label:'parcel-centre'},\n" +

                "db1 = {name:'delivery-base-1', _label:'delivery-base'},\n" +
                "db2 = {name:'delivery-base-2', _label:'delivery-base'},\n" +
                "db3 = {name:'delivery-base-3', _label:'delivery-base'},\n" +

                "da1 = {name:'delivery-area-1', _label:'delivery-area'},\n" +
                "da2 = {name:'delivery-area-2', _label:'delivery-area'},\n" +
                "da3 = {name:'delivery-area-3', _label:'delivery-area'},\n" +
                "da4 = {name:'delivery-area-4', _label:'delivery-area'},\n" +

                "ds1 = {name:'delivery-segment-1', _label:'delivery-segment'},\n" +
                "ds2 = {name:'delivery-segment-2', _label:'delivery-segment'},\n" +
                "ds3 = {name:'delivery-segment-3', _label:'delivery-segment'},\n" +
                "ds4 = {name:'delivery-segment-4', _label:'delivery-segment'},\n" +
                "ds5 = {name:'delivery-segment-5', _label:'delivery-segment'},\n" +
                "ds6 = {name:'delivery-segment-6', _label:'delivery-segment'},\n" +
                "ds7 = {name:'delivery-segment-7', _label:'delivery-segment'},\n" +
                "ds8 = {name:'delivery-segment-8', _label:'delivery-segment'},\n" +

                "pc1-[:CONNECTED_TO {cost:3, " + intervalProperties( interval1 ) + "}]->db1,\n" +
                "pc1-[:CONNECTED_TO {cost:3, " + intervalProperties( interval1 ) + "}]->db2,\n" +
                "pc2-[:CONNECTED_TO {cost:3, " + intervalProperties( interval1 ) + "}]->db2,\n" +
                "pc2-[:CONNECTED_TO {cost:3, " + intervalProperties( interval1 ) + "}]->db3,\n" +

                "db1-[:DELIVERY_ROUTE {cost:3, " + intervalProperties( interval1 ) + "}]->da1,\n" +
                "db1-[:DELIVERY_ROUTE {cost:3, " + intervalProperties( interval1 ) + "}]->da4,\n" +
                "db2-[:DELIVERY_ROUTE {cost:5, " + intervalProperties( interval1 ) + "}]->da3,\n" +
                "db3-[:DELIVERY_ROUTE {cost:3, " + intervalProperties( interval1 ) + "}]->da2,\n" +

                "da1-[:DELIVERY_ROUTE {cost:3, " + intervalProperties( interval1 ) + "}]->ds1,\n" +
                "da1-[:DELIVERY_ROUTE {cost:3, " + intervalProperties( interval1 ) + "}]->ds2,\n" +
                "da2-[:DELIVERY_ROUTE {cost:3, " + intervalProperties( interval1 ) + "}]->ds3,\n" +
                "da2-[:DELIVERY_ROUTE {cost:3, " + intervalProperties( interval1 ) + "}]->ds4,\n" +
                "da4-[:DELIVERY_ROUTE {cost:3, " + intervalProperties( interval1 ) + "}]->ds5,\n" +
                "da3-[:DELIVERY_ROUTE {cost:3, " + intervalProperties( interval1 ) + "}]->ds6,\n" +
                "da1-[:DELIVERY_ROUTE {cost:3, " + intervalProperties( interval1 ) + "}]->ds7,\n" +
                "da1-[:DELIVERY_ROUTE {cost:3, " + intervalProperties( interval1 ) + "}]->ds8,\n" +

                "pc1-[:CONNECTED_TO {cost:2, " + intervalProperties( interval2 ) + "}]->db1,\n" +
                "pc1-[:CONNECTED_TO {cost:2, " + intervalProperties( interval2 ) + "}]->db2,\n" +
                "pc2-[:CONNECTED_TO {cost:2, " + intervalProperties( interval2 ) + "}]->db2,\n" +
                "pc2-[:CONNECTED_TO {cost:2, " + intervalProperties( interval2 ) + "}]->db3,\n" +

                //Parcel centre connected directly to db1 for 2nd interval
                "pc2-[:CONNECTED_TO {cost:5, " + intervalProperties( interval2 ) + "}]->db1,\n" +

                "db1-[:DELIVERY_ROUTE {cost:2, " + intervalProperties( interval2 ) + "}]->da1,\n" +
                "db2-[:DELIVERY_ROUTE {cost:2, " + intervalProperties( interval2 ) + "}]->da4,\n" +
                "db3-[:DELIVERY_ROUTE {cost:5, " + intervalProperties( interval2 ) + "}]->da3,\n" +
                "db3-[:DELIVERY_ROUTE {cost:2, " + intervalProperties( interval2 ) + "}]->da2,\n" +

                "da4-[:DELIVERY_ROUTE {cost:2, " + intervalProperties( interval2 ) + "}]->ds1,\n" +
                "da4-[:DELIVERY_ROUTE {cost:2, " + intervalProperties( interval2 ) + "}]->ds2,\n" +
                "da2-[:DELIVERY_ROUTE {cost:2, " + intervalProperties( interval2 ) + "}]->ds3,\n" +
                "da2-[:DELIVERY_ROUTE {cost:2, " + intervalProperties( interval2 ) + "}]->ds4,\n" +
                "da3-[:DELIVERY_ROUTE {cost:2, " + intervalProperties( interval2 ) + "}]->ds5,\n" +
                "da3-[:DELIVERY_ROUTE {cost:2, " + intervalProperties( interval2 ) + "}]->ds6,\n" +
                "da4-[:DELIVERY_ROUTE {cost:2, " + intervalProperties( interval2 ) + "}]->ds7,\n" +
                "da4-[:DELIVERY_ROUTE {cost:2, " + intervalProperties( interval2 ) + "}]->ds8,\n" +

                "pc1-[:CONNECTED_TO {cost:6, " + intervalProperties( interval3 ) + "}]->db1,\n" +
                "pc1-[:CONNECTED_TO {cost:6, " + intervalProperties( interval3 ) + "}]->db2,\n" +
                "pc2-[:CONNECTED_TO {cost:6, " + intervalProperties( interval3 ) + "}]->db2,\n" +
                "pc1-[:CONNECTED_TO {cost:6, " + intervalProperties( interval3 ) + "}]->db3,\n" +

                "db1-[:DELIVERY_ROUTE {cost:6, " + intervalProperties( interval3 ) + "}]->da1,\n" +
                "db2-[:DELIVERY_ROUTE {cost:6, " + intervalProperties( interval3 ) + "}]->da4,\n" +
                "db3-[:DELIVERY_ROUTE {cost:5, " + intervalProperties( interval3 ) + "}]->da3,\n" +
                "db2-[:DELIVERY_ROUTE {cost:6, " + intervalProperties( interval3 ) + "}]->da2,\n" +

                "da1-[:DELIVERY_ROUTE {cost:6, " + intervalProperties( interval3 ) + "}]->ds1,\n" +
                "da1-[:DELIVERY_ROUTE {cost:6, " + intervalProperties( interval3 ) + "}]->ds2,\n" +
                //Different delivery centre for ds3 for 3rd interval
                "da3-[:DELIVERY_ROUTE {cost:6, " + intervalProperties( interval3 ) + "}]->ds3,\n" +
                "da3-[:DELIVERY_ROUTE {cost:6, " + intervalProperties( interval3 ) + "}]->ds4,\n" +
                "da3-[:DELIVERY_ROUTE {cost:6, " + intervalProperties( interval3 ) + "}]->ds5,\n" +
                "da3-[:DELIVERY_ROUTE {cost:6, " + intervalProperties( interval3 ) + "}]->ds6,\n" +
                "da4-[:DELIVERY_ROUTE {cost:6, " + intervalProperties( interval3 ) + "}]->ds7,\n" +
                "da4-[:DELIVERY_ROUTE {cost:6, " + intervalProperties( interval3 ) + "}]->ds8";

        return createFromCypher(
                "Logistics",
                cypher,
                indexParam( "parcel-centre", "location", "name" ),
                indexParam( "delivery-base", "location", "name" ),
                indexParam( "delivery-area", "location", "name" ),
                indexParam( "delivery-segment", "location", "name" ) );
    }

    private static String intervalProperties( Interval interval )
    {
        StringBuilder builder = new StringBuilder();
        builder.append( "start_date:" );
        builder.append( interval.getStartMillis() );
        builder.append( ", end_date:" );
        builder.append( interval.getEndMillis() );
        return builder.toString();
    }
}
