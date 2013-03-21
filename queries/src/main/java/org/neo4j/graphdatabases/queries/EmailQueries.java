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
        String query = "START bob=node:user(username='Bob') \n" +
                "MATCH (bob)-[:SENT]->(email)-[:CC]->(alias),\n" +
                "      (alias)-[:ALIAS_OF]->(bob)\n" +
                "RETURN email";

        Map<String, Object> params = new HashMap<String, Object>();

        return executionEngineWrapper.execute( query, params );
    }

    public ExecutionResult suspectBehaviour2()
    {
        String query = "START email = node:email(id = '6')\n" +
                "MATCH p=(email)<-[:REPLY_TO*1..4]-()<-[:SENT]-(replier)\n" +
                "RETURN replier.username AS replier, LENGTH(p) - 1 AS depth ORDER BY depth";

        Map<String, Object> params = new HashMap<String, Object>();

        return executionEngineWrapper.execute( query, params );
    }

    public ExecutionResult suspectBehaviour3()
    {
        String query = "START email = node:email(id = '11')\n" +
                "MATCH (email)<-[f:FORWARD_OF*]-() \n" +
                "RETURN COUNT(f)";

        Map<String, Object> params = new HashMap<String, Object>();

        return executionEngineWrapper.execute( query, params );
    }

    public ExecutionResult lossyDb()
    {
        String query = "START bob=node:user(username='Bob'), \n" +
                "      charlie=node:user(username='Charlie')\n" +
                "MATCH (bob)-[e:EMAILED]->(charlie)\n" +
                "RETURN e";

        Map<String, Object> params = new HashMap<String, Object>();

        return executionEngineWrapper.execute( query, params );
    }
}
