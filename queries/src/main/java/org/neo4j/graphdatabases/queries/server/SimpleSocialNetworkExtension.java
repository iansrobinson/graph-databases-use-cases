package org.neo4j.graphdatabases.queries.server;

import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.core.Context;

import org.neo4j.cypher.javacompat.ExecutionResult;
import org.neo4j.graphdatabases.queries.SimpleSocialNetworkQueries;
import org.neo4j.graphdb.GraphDatabaseService;

@Path("/distance")
public class SimpleSocialNetworkExtension
{
    private final  SimpleSocialNetworkQueries queries;

    public SimpleSocialNetworkExtension( @Context GraphDatabaseService db )
    {
        this.queries = new SimpleSocialNetworkQueries( db );
    }

    @GET
    @Path("/{name1}/{name2}")
    public String getDistance  ( @PathParam("name1") String name1, @PathParam("name2") String name2 )
    {
        ExecutionResult result = queries.pathBetweenTwoFriends( name1, name2 );

        return String.valueOf( result.columnAs( "depth" ).next() );
    }
}
