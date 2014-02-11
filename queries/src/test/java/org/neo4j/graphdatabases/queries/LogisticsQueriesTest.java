package org.neo4j.graphdatabases.queries;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.neo4j.graphdatabases.queries.helpers.Db.createFromCypher;
import static org.neo4j.graphdatabases.queries.testing.IndexParam.indexParam;

import java.util.Iterator;

import org.joda.time.DateTime;
import org.joda.time.Interval;
import org.junit.*;
import org.junit.rules.TestName;
import org.neo4j.cypher.javacompat.ExecutionResult;
import org.neo4j.graphdatabases.queries.helpers.PrintingExecutionEngineWrapper;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Transaction;

public class LogisticsQueriesTest
{
    @Rule
    public TestName name = new TestName();

    private GraphDatabaseService db;
    private LogisticsQueries queries;

    private static Interval interval1 = Interval.parse( "2012-10-15T00:00:00.000+01:00/2012-10-22T00:00:00.000+01:00" );
    private static Interval interval2 = Interval.parse( "2012-10-22T00:00:00.000+01:00/2012-10-29T00:00:00.000+01:00" );
    private static Interval interval3 = Interval.parse( "2012-10-29T00:00:00.000+01:00/2012-11-05T00:00:00.000+01:00" );
    private Transaction tx;


    @Before
    public void init()
    {
        try
        {

            db = createDatabase();
            queries = new LogisticsQueries( db, new PrintingExecutionEngineWrapper( db, "logistics", name ) );
            tx = db.beginTx();
        }
        catch ( Exception e )
        {
            e.printStackTrace();
            System.out.println( e.getMessage() );
        }
    }

    @After
    public void shutdown()
    {
        if ( tx != null )
        {
            tx.success();
            tx.close();
        }
        db.shutdown();
    }

    @Test
    public void parcelRoutingUsingParcelRouteCalculator() throws Exception
    {
        // given
        DateTime startDtm = interval1.getStart().plusDays( 2 );
        Interval queryInterval = new Interval( startDtm, startDtm.plusDays( 1 ) );

        // when
        Iterable<Node> results = queries.findShortestPathWithParcelRouteCalculator( "DeliveryArea-1",
                "DeliverySegment-3",
                queryInterval );


        // then
        Iterator<Node> iterator = results.iterator();


        assertEquals( "DeliveryArea-1", iterator.next().getProperty( "name" ) );
        assertEquals( "DeliveryBase-1", iterator.next().getProperty( "name" ) );
        assertEquals( "ParcelCentre-1", iterator.next().getProperty( "name" ) );
        assertEquals( "DeliveryBase-2", iterator.next().getProperty( "name" ) );
        assertEquals( "ParcelCentre-2", iterator.next().getProperty( "name" ) );
        assertEquals( "DeliveryBase-3", iterator.next().getProperty( "name" ) );
        assertEquals( "DeliveryArea-2", iterator.next().getProperty( "name" ) );
        assertEquals( "DeliverySegment-3", iterator.next().getProperty( "name" ) );

        assertFalse( iterator.hasNext() );
    }

    @Test
    public void parcelRoutingUsingParcelRouteCalculatorChoosesShortestRouteBetweenDeliveryBases() throws Exception
    {
        // given
        DateTime startDtm = interval2.getStart().plusDays( 2 );
        Interval queryInterval = new Interval( startDtm, startDtm.plusDays( 1 ) );

        // when
        Iterable<Node> results = queries.findShortestPathWithParcelRouteCalculator( "DeliveryArea-1",
                "DeliverySegment-3",
                queryInterval );


        // then
        Iterator<Node> iterator = results.iterator();


        assertEquals( "DeliveryArea-1", iterator.next().getProperty( "name" ) );
        assertEquals( "DeliveryBase-1", iterator.next().getProperty( "name" ) );
        assertEquals( "ParcelCentre-2", iterator.next().getProperty( "name" ) );
        assertEquals( "DeliveryBase-3", iterator.next().getProperty( "name" ) );
        assertEquals( "DeliveryArea-2", iterator.next().getProperty( "name" ) );
        assertEquals( "DeliverySegment-3", iterator.next().getProperty( "name" ) );

        assertFalse( iterator.hasNext() );
    }

    @Test
    public void parcelRoutingUsingParcelRouteCalculatorRespectsIntervals() throws Exception
    {
        // given
        DateTime startDtm = interval3.getStart().plusDays( 2 );
        Interval queryInterval = new Interval( startDtm, startDtm.plusDays( 1 ) );

        // when
        Iterable<Node> results = queries.findShortestPathWithParcelRouteCalculator( "DeliveryArea-1",
                "DeliverySegment-3",
                queryInterval );


        // then
        Iterator<Node> iterator = results.iterator();


        assertEquals( "DeliveryArea-1", iterator.next().getProperty( "name" ) );
        assertEquals( "DeliveryBase-1", iterator.next().getProperty( "name" ) );
        assertEquals( "ParcelCentre-1", iterator.next().getProperty( "name" ) );
        assertEquals( "DeliveryBase-3", iterator.next().getProperty( "name" ) );
        assertEquals( "DeliveryArea-3", iterator.next().getProperty( "name" ) );
        assertEquals( "DeliverySegment-3", iterator.next().getProperty( "name" ) );

        assertFalse( iterator.hasNext() );
    }

    @Test
    public void parcelRoutingUsingSimpleParcelRouteCalculatorRespectsIntervals() throws Exception
    {
        // given
        DateTime startDtm = interval3.getStart().plusDays( 2 );
        Interval queryInterval = new Interval( startDtm, startDtm.plusDays( 1 ) );

        // when
        Iterable<Node> results = queries.findShortestPathWithSimpleParcelRouteCalculator( "DeliveryArea-1",
                "DeliverySegment-3",
                queryInterval );


        // then
        Iterator<Node> iterator = results.iterator();


        assertEquals( "DeliveryArea-1", iterator.next().getProperty( "name" ) );
        assertEquals( "DeliveryBase-1", iterator.next().getProperty( "name" ) );
        assertEquals( "ParcelCentre-1", iterator.next().getProperty( "name" ) );
        assertEquals( "DeliveryBase-3", iterator.next().getProperty( "name" ) );
        assertEquals( "DeliveryArea-3", iterator.next().getProperty( "name" ) );
        assertEquals( "DeliverySegment-3", iterator.next().getProperty( "name" ) );

        assertFalse( iterator.hasNext() );
    }

    @Test
    public void withinScopeOfSingleParcelCentreParcelRouteCalculator() throws Exception
    {
        // given
        DateTime startDtm = interval2.getStart().plusDays( 2 );
        Interval queryInterval = new Interval( startDtm, startDtm.plusDays( 1 ) );

        // when
        Iterable<Node> results = queries.findShortestPathWithParcelRouteCalculator( "DeliveryArea-1",
                "DeliverySegment-8",
                queryInterval );


        // then
        Iterator<Node> iterator = results.iterator();


        assertEquals( "DeliveryArea-1", iterator.next().getProperty( "name" ) );
        assertEquals( "DeliveryBase-1", iterator.next().getProperty( "name" ) );
        assertEquals( "ParcelCentre-1", iterator.next().getProperty( "name" ) );
        assertEquals( "DeliveryBase-2", iterator.next().getProperty( "name" ) );
        assertEquals( "DeliveryArea-4", iterator.next().getProperty( "name" ) );
        assertEquals( "DeliverySegment-8", iterator.next().getProperty( "name" ) );

        assertFalse( iterator.hasNext() );
    }

    @Test
    public void parcelRoutingUsingCypherReduce() throws Exception
    {
        // given
        DateTime startDtm = interval1.getStart().plusDays( 2 );
        Interval queryInterval = new Interval( startDtm, startDtm.plusDays( 1 ) );

        // when
        ExecutionResult result = queries.findShortestPathWithCypherReduce( "DeliveryArea-1", "DeliverySegment-3",
                queryInterval );


        // then
        Iterator<Iterable<Node>> rows = result.columnAs( "n" );
        Iterator<Node> nodes = rows.next().iterator();


        assertEquals( "DeliveryArea-1", nodes.next().getProperty( "name" ) );
        assertEquals( "DeliveryBase-1", nodes.next().getProperty( "name" ) );
        assertEquals( "ParcelCentre-1", nodes.next().getProperty( "name" ) );
        assertEquals( "DeliveryBase-2", nodes.next().getProperty( "name" ) );
        assertEquals( "ParcelCentre-2", nodes.next().getProperty( "name" ) );
        assertEquals( "DeliveryBase-3", nodes.next().getProperty( "name" ) );
        assertEquals( "DeliveryArea-2", nodes.next().getProperty( "name" ) );
        assertEquals( "DeliverySegment-3", nodes.next().getProperty( "name" ) );

        assertFalse( nodes.hasNext() );
    }

    @Test
    public void parcelRoutingUsingCypherReduceChoosesShortestRouteBetweenDeliveryBases() throws Exception
    {
        // given
        DateTime startDtm = interval2.getStart().plusDays( 2 );
        Interval queryInterval = new Interval( startDtm, startDtm.plusDays( 1 ) );

        // when
        ExecutionResult result = queries.findShortestPathWithCypherReduce( "DeliveryArea-1", "DeliverySegment-3",
                queryInterval );


        // then
        Iterator<Iterable<Node>> rows = result.columnAs( "n" );
        Iterator<Node> nodes = rows.next().iterator();

        assertEquals( "DeliveryArea-1", nodes.next().getProperty( "name" ) );
        assertEquals( "DeliveryBase-1", nodes.next().getProperty( "name" ) );
        assertEquals( "ParcelCentre-2", nodes.next().getProperty( "name" ) );
        assertEquals( "DeliveryBase-3", nodes.next().getProperty( "name" ) );
        assertEquals( "DeliveryArea-2", nodes.next().getProperty( "name" ) );
        assertEquals( "DeliverySegment-3", nodes.next().getProperty( "name" ) );

        assertFalse( nodes.hasNext() );
    }

    @Test
    public void parcelRoutingUsingCypherReduceRespectsIntervals() throws Exception
    {
        // given
        DateTime startDtm = interval3.getStart().plusDays( 2 );
        Interval queryInterval = new Interval( startDtm, startDtm.plusDays( 1 ) );

        // when
        ExecutionResult result = queries.findShortestPathWithCypherReduce( "DeliveryArea-1", "DeliverySegment-3",
                queryInterval );


        // then
        Iterator<Iterable<Node>> rows = result.columnAs( "n" );
        Iterator<Node> nodes = rows.next().iterator();

        assertEquals( "DeliveryArea-1", nodes.next().getProperty( "name" ) );
        assertEquals( "DeliveryBase-1", nodes.next().getProperty( "name" ) );
        assertEquals( "ParcelCentre-1", nodes.next().getProperty( "name" ) );
        assertEquals( "DeliveryBase-3", nodes.next().getProperty( "name" ) );
        assertEquals( "DeliveryArea-3", nodes.next().getProperty( "name" ) );
        assertEquals( "DeliverySegment-3", nodes.next().getProperty( "name" ) );

        assertFalse( nodes.hasNext() );
    }

    @Test
    public void withinScopeOfSingleParcelCentreCypher() throws Exception
    {
        // given
        DateTime startDtm = interval2.getStart().plusDays( 2 );
        Interval queryInterval = new Interval( startDtm, startDtm.plusDays( 1 ) );

        // when
        ExecutionResult result = queries.findShortestPathWithCypherReduce( "DeliveryArea-1", "DeliverySegment-8",
                queryInterval );


        // then
        Iterator<Iterable<Node>> rows = result.columnAs( "n" );
        Iterator<Node> nodes = rows.next().iterator();

        assertEquals( "DeliveryArea-1", nodes.next().getProperty( "name" ) );
        assertEquals( "DeliveryBase-1", nodes.next().getProperty( "name" ) );
        assertEquals( "ParcelCentre-1", nodes.next().getProperty( "name" ) );
        assertEquals( "DeliveryBase-2", nodes.next().getProperty( "name" ) );
        assertEquals( "DeliveryArea-4", nodes.next().getProperty( "name" ) );
        assertEquals( "DeliverySegment-8", nodes.next().getProperty( "name" ) );


        assertFalse( nodes.hasNext() );
    }

    private static GraphDatabaseService createDatabase()
    {
        String cypher = "CREATE\n" +
                "(pc1:ParcelCentre:Location {name:'ParcelCentre-1'}),\n" +
                "(pc2:ParcelCentre:Location {name:'ParcelCentre-2'}),\n" +

                "(db1:DeliveryBase:Location {name:'DeliveryBase-1'}),\n" +
                "(db2:DeliveryBase:Location {name:'DeliveryBase-2'}),\n" +
                "(db3:DeliveryBase:Location {name:'DeliveryBase-3'}),\n" +

                "(da1:DeliveryArea:Location {name:'DeliveryArea-1'}),\n" +
                "(da2:DeliveryArea:Location {name:'DeliveryArea-2'}),\n" +
                "(da3:DeliveryArea:Location {name:'DeliveryArea-3'}),\n" +
                "(da4:DeliveryArea:Location {name:'DeliveryArea-4'}),\n" +

                "(ds1:DeliverySegment:Location {name:'DeliverySegment-1'}),\n" +
                "(ds2:DeliverySegment:Location {name:'DeliverySegment-2'}),\n" +
                "(ds3:DeliverySegment:Location {name:'DeliverySegment-3'}),\n" +
                "(ds4:DeliverySegment:Location {name:'DeliverySegment-4'}),\n" +
                "(ds5:DeliverySegment:Location {name:'DeliverySegment-5'}),\n" +
                "(ds6:DeliverySegment:Location {name:'DeliverySegment-6'}),\n" +
                "(ds7:DeliverySegment:Location {name:'DeliverySegment-7'}),\n" +
                "(ds8:DeliverySegment:Location {name:'DeliverySegment-8'}),\n" +

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
                indexParam( "Location", "name" ),
                indexParam( "ParcelCentre", "name" ),
                indexParam( "DeliveryBase", "name" ),
                indexParam( "DeliveryArea", "name" ),
                indexParam( "DeliverySegment", "name" ) );
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
