package org.neo4j.graphdatabases.queries;

import java.util.HashMap;
import java.util.Map;

import org.neo4j.cypher.javacompat.ExecutionEngine;
import org.neo4j.cypher.javacompat.ExecutionResult;
import org.neo4j.graphdb.GraphDatabaseService;

public class SimpleSocialNetworkQueries
{
    private final ExecutionEngine executionEngine;

    public SimpleSocialNetworkQueries( GraphDatabaseService db )
    {
        this.executionEngine = new ExecutionEngine( db );
    }

    public ExecutionResult pathBetweenTwoFriends( String firstUser, String secondUser )
    {
        String query = "MATCH (first:User{name:{firstUser}}),\n" +
                " (second:User{name:{secondUser}})\n" +
                "MATCH p=shortestPath(first-[*..4]-second)\n" +
                "RETURN length(p) AS depth";

        Map<String, Object> params = new HashMap<String, Object>();
        params.put( "firstUser", firstUser );
        params.put( "secondUser", secondUser );

        return executionEngine.execute( query, params );
    }

    public ExecutionResult friendOfAFriendToDepth4(String name)
    {
        String query =
            "MATCH (person:User {name:{name}})-[:FRIEND]-()-[:FRIEND]-()-[:FRIEND]-()-[:FRIEND]-(friend)\n" +
            "RETURN friend.name AS name";

        Map<String, Object> params = new HashMap<String, Object>();
        params.put( "name", name );

        return executionEngine.execute( query, params );

    }

}
