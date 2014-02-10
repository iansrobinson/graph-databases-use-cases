package org.neo4j.graphdatabases.queries;

import java.util.HashMap;
import java.util.Map;

import org.neo4j.cypher.javacompat.ExecutionResult;
import org.neo4j.graphdatabases.queries.helpers.ExecutionEngineWrapper;
import org.neo4j.graphdb.GraphDatabaseService;

public class EmailQueries
{
    private final GraphDatabaseService db;
    private final ExecutionEngineWrapper executionEngineWrapper;

    public EmailQueries( GraphDatabaseService db, ExecutionEngineWrapper executionEngineWrapper )
    {
        this.db = db;
        this.executionEngineWrapper = executionEngineWrapper;
    }

    public ExecutionResult suspectBehaviour()
    {
        String query =
                "MATCH (bob:User {username:'Bob'})-[:SENT]->(email)-[:CC]->(alias),\n" +
                "      (alias)-[:ALIAS_OF]->(bob)\n" +
                "RETURN email.id";

        Map<String, Object> params = new HashMap<String, Object>();

        return executionEngineWrapper.execute( query, params );
    }

    public ExecutionResult suspectBehaviour2()
    {
        String query =
                "MATCH p=(email:Email {id:'6'})<-[:REPLY_TO*1..4]-()<-[:SENT]-(replier)\n" +
                "RETURN replier.username AS replier, length(p) - 1 AS depth ORDER BY depth";

        Map<String, Object> params = new HashMap<String, Object>();

        return executionEngineWrapper.execute( query, params );
    }

    public ExecutionResult suspectBehaviour3()
    {
        String query =
                "MATCH (email:Email {id:'11'})<-[f:FORWARD_OF*]-() \n" +
                "RETURN count(f)";

        Map<String, Object> params = new HashMap<String, Object>();

        return executionEngineWrapper.execute( query, params );
    }

    public ExecutionResult lossyDb()
    {
        String query =
                "MATCH (bob:User {username:'Bob'})-[e:EMAILED]->(charlie:User {username:'Charlie'})\n" +
                "RETURN e";

        Map<String, Object> params = new HashMap<String, Object>();

        return executionEngineWrapper.execute( query, params );
    }
}
