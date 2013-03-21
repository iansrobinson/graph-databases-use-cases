package org.neo4j.graphdatabases.queries;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.neo4j.graphdatabases.queries.helpers.Db.createFromCypher;

import java.util.Iterator;
import java.util.Map;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestName;
import org.neo4j.cypher.javacompat.ExecutionResult;
import org.neo4j.graphdatabases.queries.helpers.PrintingExecutionEngineWrapper;
import org.neo4j.graphdatabases.queries.testing.IndexParam;
import org.neo4j.graphdb.GraphDatabaseService;

public class ShakespeareQueriesTest
{
    @Rule
    public static TestName name = new TestName();

    private static GraphDatabaseService db;
    private static ShakespeareQueries queries;

    @BeforeClass
    public static void init()
    {
        db = createDatabase();
        queries = new ShakespeareQueries( db, new PrintingExecutionEngineWrapper( db, "shakespeare", name ) );
    }


    @AfterClass
    public static void shutdown()
    {
        db.shutdown();
    }

    @Test
    public void theatreCityBard() throws Exception
    {
        ExecutionResult result = queries.theatreCityBard();

        System.out.println( result.dumpToString() );
    }

    @Test
        public void exampleOfWith() throws Exception
        {
            ExecutionResult result = queries.exampleOfWith();

            System.out.println( result.dumpToString() );
        }

    @Test
    public void shouldReturnAllPlays() throws Exception
    {
        ExecutionResult result = queries.allPlays();

        Iterator<String> plays = result.columnAs( "play" );

        assertEquals( "Julius Caesar", plays.next() );
        assertEquals( "The Tempest", plays.next() );
        assertFalse( plays.hasNext() );

    }

    @Test
    public void shouldReturnLatePeriodPlays() throws Exception
    {
        ExecutionResult result = queries.latePeriodPlays();


        Iterator<String> plays = result.columnAs( "play" );

        assertEquals( "The Tempest", plays.next() );
        assertFalse( plays.hasNext() );

    }

    @Test
    public void orderedByPerformance() throws Exception
    {
        ExecutionResult result = queries.orderedByPerformance();

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
        String cypher = "CREATE shakespeare = { firstname: 'William', lastname: 'Shakespeare', _label: 'author' },\n" +
                "       juliusCaesar = { title: 'Julius Caesar', _label: 'play' },\n" +
                "       (shakespeare)-[:WROTE_PLAY { year: 1599 }]->(juliusCaesar),\n" +
                "       theTempest = { title: 'The Tempest', _label: 'play' },\n" +
                "       (shakespeare)-[:WROTE_PLAY { year: 1610}]->(theTempest),\n" +
                "       rsc = { name: 'RSC', _label: 'company' },\n" +
                "       production1 = { name: 'Julius Caesar', _label: 'production' },\n" +
                "       (rsc)-[:PRODUCED]->(production1),\n" +
                "       (production1)-[:PRODUCTION_OF]->(juliusCaesar),\n" +
                "       performance1 = { date: 20120729, _label: 'performance' },\n" +
                "       (performance1)-[:PERFORMANCE_OF]->(production1),\n" +
                "       production2 = { name: 'The Tempest', _label: 'production' },\n" +
                "       (rsc)-[:PRODUCED]->(production2),\n" +
                "       (production2)-[:PRODUCTION_OF]->(theTempest),\n" +
                "       performance2 = { date: 20061121, _label: 'performance' },\n" +
                "       (performance2)-[:PERFORMANCE_OF]->(production2),\n" +
                "       performance3 = { date: 20120730, _label: 'performance' },\n" +
                "       (performance3)-[:PERFORMANCE_OF]->(production1),\n" +
                "       billy = { name: 'Billy', _label: 'user' },\n" +
                "       review = { rating: 5, review: 'This was awesome!', _label: 'review' },\n" +
                "       (billy)-[:WROTE_REVIEW]->(review),\n" +
                "       (review)-[:RATED]->(performance1),\n" +
                "       theatreRoyal = { name: 'Theatre Royal', _label: 'venue' },\n" +
                "       (performance1)-[:VENUE]->(theatreRoyal),\n" +
                "       (performance2)-[:VENUE]->(theatreRoyal),\n" +
                "       (performance3)-[:VENUE]->(theatreRoyal),\n" +
                "       greyStreet = { name: 'Grey Street', _label: 'street' },\n" +
                "       (theatreRoyal)-[:STREET]->(greyStreet),\n" +
                "       newcastle = { name: 'Newcastle', _label: 'city' },\n" +
                "       (greyStreet)-[:CITY]->(newcastle),\n" +
                "       tyneAndWear = { name: 'Tyne and Wear', _label: 'county' },\n" +
                "       (newcastle)-[:COUNTY]->(tyneAndWear),\n" +
                "       england = { name: 'England', _label: 'country' },\n" +
                "       (tyneAndWear)-[:COUNTRY]->(england),\n" +
                "       stratford = { name: 'Stratford upon Avon', _label: 'city' },\n" +
                "       (stratford)-[:COUNTRY]->(england),\n" +
                "       (rsc)-[:BASED_IN]->(stratford),\n" +
                "       (shakespeare)-[:BORN_IN]->stratford";

        return createFromCypher(
                "Shakespeare",
                cypher,
                IndexParam.indexParam( "venue", "name" ),
                IndexParam.indexParam( "author", "lastname" ),
                IndexParam.indexParam( "city", "name" ) );
    }

}
