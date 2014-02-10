package org.neo4j.graphdatabases.queries;

import java.util.Iterator;
import java.util.Map;

import org.junit.*;
import org.junit.rules.TestName;

import org.neo4j.cypher.javacompat.ExecutionResult;
import org.neo4j.graphdatabases.queries.helpers.Db;
import org.neo4j.graphdatabases.queries.helpers.PrintingExecutionEngineWrapper;
import org.neo4j.graphdatabases.queries.testing.IndexParam;
import org.neo4j.graphdb.*;
import org.neo4j.graphdb.index.Index;

import static java.util.Arrays.asList;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;

import static org.neo4j.graphdatabases.queries.helpers.Db.createFromCypherWithAutoIndexing;
import static org.neo4j.graphdb.DynamicLabel.label;
import static org.neo4j.graphdb.DynamicRelationshipType.withName;

public class ShakespeareQueriesTest
{
    @Rule
    public TestName name = new TestName();

    private GraphDatabaseService db;
    private GraphDatabaseService dbUsingCoreApi;
    private ShakespeareQueries queries;
    private ShakespeareQueries queries2;
    private ShakespeareQueriesUsingAutoIndexes queriesUsingAutoIndexes;

    @Before
    public void init()
    {
        db = createDatabase();
        dbUsingCoreApi = createDatabaseUsingCoreApi();
        queries = new ShakespeareQueries( new PrintingExecutionEngineWrapper( db, "shakespeare", name ) );
        queries2 = new ShakespeareQueries( new PrintingExecutionEngineWrapper( dbUsingCoreApi, "shakespeare", name ) );
        queriesUsingAutoIndexes = new ShakespeareQueriesUsingAutoIndexes(
                new PrintingExecutionEngineWrapper( db, "shakespeare-auot-indexes", name ) );
    }

    @After
    public void shutdown()
    {
        db.shutdown();
        dbUsingCoreApi.shutdown();
    }

    @Test
    public void theatreCityBard() throws Exception
    {
        try ( Transaction tx = db.beginTx() )
        {
            assertTheatreCityBard( queries.theatreCityBard() );
            assertTheatreCityBard( queries2.theatreCityBard() );
            assertTheatreCityBard( queriesUsingAutoIndexes.theatreCityBard() );
            tx.success();
        }
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
        try ( Transaction tx = db.beginTx() )
        {
            assertExampleOfWith( queries.exampleOfWith() );
            assertExampleOfWith( queries2.exampleOfWith() );
            assertExampleOfWith( queriesUsingAutoIndexes.exampleOfWith() );
            tx.success();
        }
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
        try ( Transaction tx = db.beginTx() )
        {
            assertAllPlays( queries.allPlays() );
            assertAllPlays( queries2.allPlays() );
            assertAllPlays( queriesUsingAutoIndexes.allPlays() );
        }
    }

    private void assertAllPlays( ExecutionResult result )
    {
        try ( Transaction tx = db.beginTx() )
        {
            Iterator<String> plays = result.columnAs( "play" );

            assertEquals( "Julius Caesar", plays.next() );
            assertEquals( "The Tempest", plays.next() );
            assertFalse( plays.hasNext() );
            tx.success();
        }
    }

    @Test
    public void shouldReturnLatePeriodPlays() throws Exception
    {
        try ( Transaction tx = db.beginTx() )
        {
            assertLatePeriodPlays( queries.latePeriodPlays() );
            assertLatePeriodPlays( queries2.latePeriodPlays() );
            assertLatePeriodPlays( queriesUsingAutoIndexes.latePeriodPlays() );
            tx.success();
        }
    }

    private void assertLatePeriodPlays( ExecutionResult result )
    {
        try ( Transaction tx = db.beginTx() )
        {
            Iterator<String> plays = result.columnAs( "play" );

            assertEquals( "The Tempest", plays.next() );
            assertFalse( plays.hasNext() );
            tx.success();
        }
    }

    @Test
    public void orderedByPerformance() throws Exception
    {
        try ( Transaction tx = db.beginTx() )
        {
            assertOrderedByPerformance( queries.orderedByPerformance() );
            assertOrderedByPerformance( queries2.orderedByPerformance() );
            assertOrderedByPerformance( queriesUsingAutoIndexes.orderedByPerformance() );
            tx.success();
        }
    }

    private void assertOrderedByPerformance( ExecutionResult result )
    {
        try ( Transaction tx = db.beginTx() )
        {
            Iterator<Map<String, Object>> plays = result.iterator();

            Map<String, Object> row = plays.next();
            assertEquals( "Julius Caesar", row.get( "play" ) );
            assertEquals( 2L, row.get( "performance_count" ) );

            row = plays.next();
            assertEquals( "The Tempest", row.get( "play" ) );
            assertEquals( 1L, row.get( "performance_count" ) );

            assertFalse( plays.hasNext() );
            tx.success();
        }
    }

    private static GraphDatabaseService createDatabase()
    {
        String cypher = "CREATE (shakespeare:Author { firstname: 'William', lastname: 'Shakespeare' }),\n" +
                "       (juliusCaesar:Play { title: 'Julius Caesar' }),\n" +
                "       (shakespeare)-[:WROTE_PLAY { year: 1599 }]->(juliusCaesar),\n" +
                "       (theTempest:Play { title: 'The Tempest' }),\n" +
                "       (shakespeare)-[:WROTE_PLAY { year: 1610}]->(theTempest),\n" +
                "       (rsc:Company { name: 'RSC' }),\n" +
                "       (production1:Production { name: 'Julius Caesar' }),\n" +
                "       (rsc)-[:PRODUCED]->(production1),\n" +
                "       (production1)-[:PRODUCTION_OF]->(juliusCaesar),\n" +
                "       (performance1:Performance { date: 20120729 }),\n" +
                "       (performance1)-[:PERFORMANCE_OF]->(production1),\n" +
                "       (production2:Production { name: 'The Tempest' }),\n" +
                "       (rsc)-[:PRODUCED]->(production2),\n" +
                "       (production2)-[:PRODUCTION_OF]->(theTempest),\n" +
                "       (performance2:Performance { date: 20061121 }),\n" +
                "       (performance2)-[:PERFORMANCE_OF]->(production2),\n" +
                "       (performance3:Performance { date: 20120730 }),\n" +
                "       (performance3)-[:PERFORMANCE_OF]->(production1),\n" +
                "       (billy:User { name: 'Billy' }),\n" +
                "       (review:Review { rating: 5, review: 'This was awesome!' }),\n" +
                "       (billy)-[:WROTE_REVIEW]->(review),\n" +
                "       (review)-[:RATED]->(performance1),\n" +
                "       (theatreRoyal:Venue { name: 'Theatre Royal' }),\n" +
                "       (performance1)-[:VENUE]->(theatreRoyal),\n" +
                "       (performance2)-[:VENUE]->(theatreRoyal),\n" +
                "       (performance3)-[:VENUE]->(theatreRoyal),\n" +
                "       (greyStreet:Street { name: 'Grey Street' }),\n" +
                "       (theatreRoyal)-[:STREET]->(greyStreet),\n" +
                "       (newcastle:City { name: 'Newcastle' }),\n" +
                "       (greyStreet)-[:CITY]->(newcastle),\n" +
                "       (tyneAndWear:County { name: 'Tyne and Wear' }),\n" +
                "       (newcastle)-[:COUNTY]->(tyneAndWear),\n" +
                "       (england:Country { name: 'England' }),\n" +
                "       (tyneAndWear)-[:COUNTRY]->(england),\n" +
                "       (stratford:City { name: 'Stratford upon Avon' }),\n" +
                "       (stratford)-[:COUNTRY]->(england),\n" +
                "       (rsc)-[:BASED_IN]->(stratford),\n" +
                "       (shakespeare)-[:BORN_IN]->stratford";

        return createFromCypherWithAutoIndexing(
                "Shakespeare",
                cypher,
                IndexParam.indexParam( "Venue", "name" ),
                IndexParam.indexParam( "Author", "lastname" ),
                IndexParam.indexParam( "City", "name" ) );
    }

    private static GraphDatabaseService createDatabaseUsingCoreApi()
    {
        GraphDatabaseService db = Db.tempDb();

        try (Transaction tx = db.beginTx())
        {
            db.schema().indexFor(label("Author")).on("lastname").create();
            db.schema().indexFor(label("City")).on("name").create();
            db.schema().indexFor(label("Venue")).on("name").create();
            tx.success();
        }
        try (Transaction tx = db.beginTx())
        {
            Node shakespeare = db.createNode(label("Author"));
            shakespeare.setProperty("firstname", "William");
            shakespeare.setProperty("lastname", "Shakespeare");

            Node juliusCaesar = db.createNode(label("Play"));
            juliusCaesar.setProperty("title", "Julius Caesar");

            Relationship wrote_play_jc = shakespeare.createRelationshipTo(juliusCaesar, withName("WROTE_PLAY"));
            wrote_play_jc.setProperty("year", 1599);

            Node tempest = db.createNode(label("Play"));
            tempest.setProperty("title", "The Tempest");

            Relationship wrote_play_t = shakespeare.createRelationshipTo(tempest, withName("WROTE_PLAY"));
            wrote_play_t.setProperty("year", 1610);

            Node rsc = db.createNode(label("Company"));
            rsc.setProperty("name", "RSC");

            Node production1 = db.createNode(label("Production"));
            production1.setProperty("name", "Julius Caesar");

            rsc.createRelationshipTo(production1, withName("PRODUCED"));
            production1.createRelationshipTo(juliusCaesar, withName("PRODUCTION_OF"));

            Node performance1 = db.createNode(label("Performance"));
            performance1.setProperty("date", 20120729);
            performance1.createRelationshipTo(production1, withName("PERFORMANCE_OF"));

            Node production2 = db.createNode(label("Production"));
            production2.setProperty("name", "The Tempest");
            production2.createRelationshipTo(tempest, withName("PRODUCTION_OF"));
            rsc.createRelationshipTo(production2, withName("PRODUCED"));

            Node performance2 = db.createNode(label("Performance"));
            performance2.setProperty("date", 20061121);
            performance2.createRelationshipTo(production2, withName("PERFORMANCE_OF"));

            Node performance3 = db.createNode(label("Performance"));
            performance3.setProperty("date", 20120730);
            performance3.createRelationshipTo(production1, withName("PERFORMANCE_OF"));

            Node billy = db.createNode(label("User"));
            billy.setProperty("name", "Billy");

            Node review = db.createNode(label("Review"));
            review.setProperty("rating", 5);
            review.setProperty("review", "This was awesome!");
            review.createRelationshipTo(performance1, withName("RATED"));
            billy.createRelationshipTo(review, withName("WROTE_REVIEW"));

            Node theatreRoyal = db.createNode(label("Venue"));
            theatreRoyal.setProperty("name", "Theatre Royal");

            performance1.createRelationshipTo(theatreRoyal, withName("VENUE"));
            performance2.createRelationshipTo(theatreRoyal, withName("VENUE"));
            performance3.createRelationshipTo(theatreRoyal, withName("VENUE"));

            Node greyStreet = db.createNode(label("Street"));
            greyStreet.setProperty("name", "Grey Street");
            theatreRoyal.createRelationshipTo(greyStreet, withName("STREET"));

            Node newcastle = db.createNode(label("City"));
            newcastle.setProperty("name", "Newcastle");
            greyStreet.createRelationshipTo(newcastle, withName("CITY"));

            Node tyneAndWear = db.createNode(label("County"));
            tyneAndWear.setProperty("name", "Tyne and Wear");
            newcastle.createRelationshipTo(tyneAndWear, withName("COUNTY"));

            Node england = db.createNode(label("Country"));
            england.setProperty("name", "England");
            tyneAndWear.createRelationshipTo(england, withName("COUNTRY"));

            Node stratford = db.createNode(label("City"));
            stratford.setProperty("name", "Stratford upon Avon");
            stratford.createRelationshipTo(england, withName("COUNTRY"));
            rsc.createRelationshipTo(stratford, withName("BASED_IN"));
            shakespeare.createRelationshipTo(stratford, withName("BORN_IN"));

            tx.success();
        }

        return db;
    }


}
