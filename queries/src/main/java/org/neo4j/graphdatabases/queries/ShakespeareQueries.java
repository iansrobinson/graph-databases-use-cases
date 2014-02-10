package org.neo4j.graphdatabases.queries;

import java.util.HashMap;
import java.util.Map;

import org.neo4j.cypher.javacompat.ExecutionResult;
import org.neo4j.graphdatabases.queries.helpers.ExecutionEngineWrapper;

public class ShakespeareQueries
{
    private final ExecutionEngineWrapper executionEngineWrapper;

    public ShakespeareQueries( ExecutionEngineWrapper executionEngineWrapper )
    {
        this.executionEngineWrapper = executionEngineWrapper;
    }

    public ExecutionResult theatreCityBard()
    {
        String query =
                "MATCH (theater:Venue {name:'Theatre Royal'}), \n" +
                        "      (newcastle:City {name:'Newcastle'}), \n" +
                        "      (bard:Author {lastname:'Shakespeare'})\n" +
                        "RETURN theater.name AS theater, newcastle.name AS city, bard.lastname AS bard";

        Map<String, Object> params = new HashMap<String, Object>();

        return executionEngineWrapper.execute( query, params );
    }

    public ExecutionResult allPlays()
    {
        String query =
                "MATCH (theater:Venue {name:'Theatre Royal'}), \n" +
                        "      (newcastle:City {name:'Newcastle'}), \n" +
                        "      (bard:Author {lastname:'Shakespeare'})\n" +
                        "MATCH (newcastle)<-[:STREET|CITY*1..2]-(theater)\n" +
                        "      <-[:VENUE]-()-[:PERFORMANCE_OF]->()-[:PRODUCTION_OF]->\n" +
                        "      (play)<-[:WROTE_PLAY]-(bard)\n" +
                        "RETURN DISTINCT play.title AS play";

        Map<String, Object> params = new HashMap<String, Object>();

        return executionEngineWrapper.execute( query, params );
    }

    public ExecutionResult latePeriodPlays()
    {
        String query =
                "MATCH (theater:Venue {name:'Theatre Royal'}), \n" +
                        "      (newcastle:City {name:'Newcastle'}), \n" +
                        "      (bard:Author {lastname:'Shakespeare'})\n" +
                        "MATCH (newcastle)<-[:STREET|CITY*1..2]-(theater)<-[:VENUE]-()-[:PERFORMANCE_OF]->()\n" +
                        "      -[:PRODUCTION_OF]->(play)<-[w:WROTE_PLAY]-(bard)\n" +
                        "WHERE w.year > 1608\n" +
                        "RETURN DISTINCT play.title AS play";

        Map<String, Object> params = new HashMap<String, Object>();

        return executionEngineWrapper.execute( query, params );
    }

    public ExecutionResult orderedByPerformance()
    {
        String query =
                "MATCH (theater:Venue {name:'Theatre Royal'}), \n" +
                        "      (newcastle:City {name:'Newcastle'}), \n" +
                        "      (bard:Author {lastname:'Shakespeare'})\n" +
                        "MATCH (newcastle)<-[:STREET|CITY*1..2]-(theater)<-[:VENUE]-()-[p:PERFORMANCE_OF]->()\n" +
                        "      -[:PRODUCTION_OF]->(play)<-[:WROTE_PLAY]-(bard)\n" +
                        "RETURN   play.title AS play, count(p) AS performance_count \n" +
                        "ORDER BY performance_count DESC";

        Map<String, Object> params = new HashMap<String, Object>();

        return executionEngineWrapper.execute( query, params );
    }

    public ExecutionResult exampleOfWith()
    {
        String query =
                       "MATCH (bard:Author {lastname:'Shakespeare'})\n" +
                               "MATCH (bard)-[w:WROTE_PLAY]->(play)\n" +
                               "WITH play \n" +
                               "ORDER BY w.year DESC \n" +
                               "RETURN collect(play.title) AS plays";

               Map<String, Object> params = new HashMap<String, Object>();

               return executionEngineWrapper.execute( query, params );
    }

}
