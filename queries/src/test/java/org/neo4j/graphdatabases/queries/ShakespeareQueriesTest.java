package org.neo4j.graphdatabases.queries;

import java.util.Iterator;
import java.util.Map;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestName;

import org.neo4j.cypher.javacompat.ExecutionResult;
import org.neo4j.graphdatabases.queries.helpers.Db;
import org.neo4j.graphdatabases.queries.helpers.PrintingExecutionEngineWrapper;
import org.neo4j.graphdatabases.queries.testing.IndexParam;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.index.Index;

import static java.util.Arrays.asList;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;

import static org.neo4j.graphdatabases.queries.helpers.Db.createFromCypherWithAutoIndexing;
import static org.neo4j.graphdb.DynamicRelationshipType.withName;

public class ShakespeareQueriesTest
{
    @Rule
    public static TestName name = new TestName();

    private static GraphDatabaseService db;
    private static GraphDatabaseService dbUsingCoreApi;
    private static ShakespeareQueries queries;
    private static ShakespeareQueries queries2;
    private static ShakespeareQueriesUsingAutoIndexes queriesUsingAutoIndexes;

    @BeforeClass
    public static void init()
    {
        db = createDatabase();
        dbUsingCoreApi = createDatabaseUsingCoreApi();
        queries = new ShakespeareQueries( new PrintingExecutionEngineWrapper( db, "shakespeare", name ) );
        queries2 = new ShakespeareQueries( new PrintingExecutionEngineWrapper( dbUsingCoreApi, "shakespeare", name ) );
        queriesUsingAutoIndexes = new ShakespeareQueriesUsingAutoIndexes(
                new PrintingExecutionEngineWrapper( db, "shakespeare-auot-indexes", name ) );
    }

    @AfterClass
    public static void shutdown()
    {
        db.shutdown();
        dbUsingCoreApi.shutdown();
    }

    @Test
    public void theatreCityBard() throws Exception
    {
        assertTheatreCityBard( queries.theatreCityBard() );
        assertTheatreCityBard( queries2.theatreCityBard() );
        assertTheatreCityBard( queriesUsingAutoIndexes.theatreCityBard() );
    }

    private void assertTheatreCityBard( ExecutionResult results )
    {
        Iterator<Map<String, Object>> iterator = results.iterator();
        Map<String, Object> result = iterator.next();

        assertEquals( "Theatre Royal", result.get( "theater" ) );
        assertEquals( "Newcastle", result.get( "city" ) );
        assertEquals( "Shakespeare", result.get( "bard" ) );

        assertFalse( iterator.hasNext() );
    }

    @Test
    public void exampleOfWith() throws Exception
    {
        assertExampleOfWith( queries.exampleOfWith() );
        assertExampleOfWith( queries2.exampleOfWith() );
        assertExampleOfWith( queriesUsingAutoIndexes.exampleOfWith() );
    }

    private void assertExampleOfWith( ExecutionResult results )
    {
        Iterator<Map<String, Object>> iterator = results.iterator();
        Map<String, Object> result = iterator.next();

        assertEquals( asList( "The Tempest", "Julius Caesar" ), result.get( "plays" ) );
        assertFalse( iterator.hasNext() );
    }

    @Test
    public void shouldReturnAllPlays() throws Exception
    {
        assertAllPlays( queries.allPlays() );
        assertAllPlays( queries2.allPlays() );
        assertAllPlays( queriesUsingAutoIndexes.allPlays() );
    }

    private void assertAllPlays( ExecutionResult result )
    {
        Iterator<String> plays = result.columnAs( "play" );

        assertEquals( "Julius Caesar", plays.next() );
        assertEquals( "The Tempest", plays.next() );
        assertFalse( plays.hasNext() );
    }

    @Test
    public void shouldReturnLatePeriodPlays() throws Exception
    {
        assertLatePeriodPlays( queries.latePeriodPlays() );
        assertLatePeriodPlays( queries2.latePeriodPlays() );
        assertLatePeriodPlays( queriesUsingAutoIndexes.latePeriodPlays() );
    }

    private void assertLatePeriodPlays( ExecutionResult result )
    {
        Iterator<String> plays = result.columnAs( "play" );

        assertEquals( "The Tempest", plays.next() );
        assertFalse( plays.hasNext() );
    }

    @Test
    public void orderedByPerformance() throws Exception
    {
        assertOrderedByPerformance( queries.orderedByPerformance() );
        assertOrderedByPerformance( queries2.orderedByPerformance() );
        assertOrderedByPerformance( queriesUsingAutoIndexes.orderedByPerformance() );
    }

    private void assertOrderedByPerformance( ExecutionResult result )
    {
        Iterator<Map<String, Object>> plays = result.iterator();

        Map<String, Object> row = plays.next();
        assertEquals( "Julius Caesar", row.get( "play" ) );
        assertEquals( 2L, row.get( "performance_count" ) );

        row = plays.next();
        assertEquals( "The Tempest", row.get( "play" ) );
        assertEquals( 1L, row.get( "performance_count" ) );

        assertFalse( plays.hasNext() );
    }

    private static GraphDatabaseService createDatabase()
    {
        String cypher = "CREATE (shakespeare { firstname: 'William', lastname: 'Shakespeare', _label: 'author' }),\n" +
                "       (juliusCaesar { title: 'Julius Caesar', _label: 'play' }),\n" +
                "       (shakespeare)-[:WROTE_PLAY { year: 1599 }]->(juliusCaesar),\n" +
                "       (theTempest { title: 'The Tempest', _label: 'play' }),\n" +
                "       (shakespeare)-[:WROTE_PLAY { year: 1610}]->(theTempest),\n" +
                "       (rsc { name: 'RSC', _label: 'company' }),\n" +
                "       (production1 { name: 'Julius Caesar', _label: 'production' }),\n" +
                "       (rsc)-[:PRODUCED]->(production1),\n" +
                "       (production1)-[:PRODUCTION_OF]->(juliusCaesar),\n" +
                "       (performance1 { date: 20120729, _label: 'performance' }),\n" +
                "       (performance1)-[:PERFORMANCE_OF]->(production1),\n" +
                "       (production2 { name: 'The Tempest', _label: 'production' }),\n" +
                "       (rsc)-[:PRODUCED]->(production2),\n" +
                "       (production2)-[:PRODUCTION_OF]->(theTempest),\n" +
                "       (performance2 { date: 20061121, _label: 'performance' }),\n" +
                "       (performance2)-[:PERFORMANCE_OF]->(production2),\n" +
                "       (performance3 { date: 20120730, _label: 'performance' }),\n" +
                "       (performance3)-[:PERFORMANCE_OF]->(production1),\n" +
                "       (billy { name: 'Billy', _label: 'user' }),\n" +
                "       (review { rating: 5, review: 'This was awesome!', _label: 'review' }),\n" +
                "       (billy)-[:WROTE_REVIEW]->(review),\n" +
                "       (review)-[:RATED]->(performance1),\n" +
                "       (theatreRoyal { name: 'Theatre Royal', _label: 'venue' }),\n" +
                "       (performance1)-[:VENUE]->(theatreRoyal),\n" +
                "       (performance2)-[:VENUE]->(theatreRoyal),\n" +
                "       (performance3)-[:VENUE]->(theatreRoyal),\n" +
                "       (greyStreet { name: 'Grey Street', _label: 'street' }),\n" +
                "       (theatreRoyal)-[:STREET]->(greyStreet),\n" +
                "       (newcastle { name: 'Newcastle', _label: 'city' }),\n" +
                "       (greyStreet)-[:CITY]->(newcastle),\n" +
                "       (tyneAndWear { name: 'Tyne and Wear', _label: 'county' }),\n" +
                "       (newcastle)-[:COUNTY]->(tyneAndWear),\n" +
                "       (england { name: 'England', _label: 'country' }),\n" +
                "       (tyneAndWear)-[:COUNTRY]->(england),\n" +
                "       (stratford { name: 'Stratford upon Avon', _label: 'city' }),\n" +
                "       (stratford)-[:COUNTRY]->(england),\n" +
                "       (rsc)-[:BASED_IN]->(stratford),\n" +
                "       (shakespeare)-[:BORN_IN]->stratford";

        return createFromCypherWithAutoIndexing(
                "Shakespeare",
                cypher,
                IndexParam.indexParam( "venue", "name" ),
                IndexParam.indexParam( "author", "lastname" ),
                IndexParam.indexParam( "city", "name" ) );
    }

    private static GraphDatabaseService createDatabaseUsingCoreApi()
    {
        GraphDatabaseService db = Db.tempDb();

        Index<Node> venueIndex = db.index().forNodes( "venue" );
        Index<Node> cityIndex = db.index().forNodes( "city" );
        Index<Node> authorIndex = db.index().forNodes( "author" );


        Transaction tx = db.beginTx();
        try
        {
            Node shakespeare = db.createNode();
            shakespeare.setProperty( "firstname", "William" );
            shakespeare.setProperty( "lastname", "Shakespeare" );
            authorIndex.add( shakespeare, "lastname", shakespeare.getProperty( "lastname" ) );

            Node juliusCaesar = db.createNode();
            juliusCaesar.setProperty( "title", "Julius Caesar" );

            Relationship wrote_play_jc = shakespeare.createRelationshipTo( juliusCaesar, withName( "WROTE_PLAY" ) );
            wrote_play_jc.setProperty( "year", 1599 );

            Node tempest = db.createNode();
            tempest.setProperty( "title", "The Tempest" );

            Relationship wrote_play_t = shakespeare.createRelationshipTo( tempest, withName( "WROTE_PLAY" ) );
            wrote_play_t.setProperty( "year", 1610 );

            Node rsc = db.createNode();
            rsc.setProperty("name", "RSC");

            Node production1 = db.createNode();
            production1.setProperty("name", "Julius Caesar");

            rsc.createRelationshipTo(production1, withName("PRODUCED"));
            production1.createRelationshipTo(juliusCaesar, withName("PRODUCTION_OF"));

            Node performance1 = db.createNode();
            performance1.setProperty("date", 20120729 );
            performance1.createRelationshipTo(production1, withName("PERFORMANCE_OF"));

            Node production2 = db.createNode();
            production2.setProperty("name", "The Tempest");
            production2.createRelationshipTo(tempest, withName("PRODUCTION_OF"));
            rsc.createRelationshipTo(production2, withName("PRODUCED"));

            Node performance2 = db.createNode();
            performance2.setProperty("date",20061121 );
            performance2.createRelationshipTo(production2, withName("PERFORMANCE_OF"));

            Node performance3 = db.createNode();
            performance3.setProperty("date",20120730 );
            performance3.createRelationshipTo(production1, withName("PERFORMANCE_OF"));

            Node billy = db.createNode();
            billy.setProperty("name", "Billy");

            Node review = db.createNode();
            review.setProperty("rating", 5);
            review.setProperty("review", "This was awesome!");
            review.createRelationshipTo(performance1, withName("RATED"));
            billy.createRelationshipTo(review, withName("WROTE_REVIEW"));

            Node theatreRoyal = db.createNode();
            theatreRoyal.setProperty("name", "Theatre Royal");
            venueIndex.add( theatreRoyal, "name", theatreRoyal.getProperty( "name" ) );

            performance1.createRelationshipTo(theatreRoyal, withName("VENUE"));
            performance2.createRelationshipTo(theatreRoyal, withName("VENUE"));
            performance3.createRelationshipTo(theatreRoyal, withName("VENUE"));

            Node greyStreet = db.createNode();
            greyStreet.setProperty("name", "Grey Street");
            theatreRoyal.createRelationshipTo(greyStreet, withName("STREET"));

            Node newcastle = db.createNode();
            newcastle.setProperty("name", "Newcastle");
            cityIndex.add( newcastle, "name", newcastle.getProperty( "name" ) );
            greyStreet.createRelationshipTo(newcastle, withName("CITY"));

            Node tyneAndWear = db.createNode();
            tyneAndWear.setProperty("name", "Tyne and Wear");
            newcastle.createRelationshipTo(tyneAndWear, withName("COUNTY"));

            Node england = db.createNode();
            england.setProperty("name", "England");
            tyneAndWear.createRelationshipTo(england, withName("COUNTRY"));

            Node stratford = db.createNode();
            stratford.setProperty("name", "Stratford upon Avon");
            stratford.createRelationshipTo(england, withName("COUNTRY"));
            rsc.createRelationshipTo(stratford, withName("BASED_IN"));
            shakespeare.createRelationshipTo(stratford, withName("BORN_IN"));

            tx.success();
        }
        finally
        {
            tx.finish();
        }

        return db;
    }


}
