package org.neo4j.graphdatabases.queries.server;

import static org.junit.Assert.assertEquals;
import static org.neo4j.graphdatabases.queries.helpers.Db.createFromCypher;

import java.io.IOException;

import javax.ws.rs.core.MediaType;

import com.sun.jersey.api.client.Client;
import com.sun.jersey.api.client.ClientResponse;
import com.sun.jersey.api.client.WebResource;
import com.sun.jersey.api.client.config.ClientConfig;
import com.sun.jersey.api.client.config.DefaultClientConfig;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.neo4j.graphdatabases.queries.testing.IndexParam;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.server.CommunityNeoServer;
import org.neo4j.server.helpers.ServerBuilder;

public class SimpleSocialNetworkExtensionTest
{
    private static CommunityNeoServer server;
    private static GraphDatabaseService db;

    @BeforeClass
    public static void init() throws IOException
    {
        server = ServerBuilder.server()
                .withThirdPartyJaxRsPackage(
                        "org.neo4j.graphdatabases.queries.server",
                        "/socnet" )
                .build();
        server.start();

        db = server.getDatabase().getGraph();
        populateDatabase( db );
    }

    @AfterClass
    public static void teardown()
    {
        server.stop();
    }

    @Test
    public void serverShouldReturnDistance() throws Exception
    {
        ClientConfig config = new DefaultClientConfig();
        Client client = Client.create( config );

        WebResource resource = client
                .resource( "http://localhost:7474/socnet/distance/Ben/Mike" );
        ClientResponse response = resource
                .accept( MediaType.TEXT_PLAIN )
                .get( ClientResponse.class );

        assertEquals( 200, response.getStatus() );
        assertEquals( "text/plain",
                response.getHeaders().get( "Content-Type" ).get( 0 ) );
        assertEquals( "4", response.getEntity( String.class ) );
    }

    @Test
    public void extensionShouldReturnDistance() throws Exception
    {
        // given
        SimpleSocialNetworkExtension extension = new SimpleSocialNetworkExtension( db );

        // when
        String distance = extension.getDistance( "Ben", "Mike" );

        // then
        assertEquals( "4", distance );
    }

    private static GraphDatabaseService populateDatabase( GraphDatabaseService db )
    {
        String cypher = "CREATE\n" +
                "(ben {name:'Ben', _label:'user'}),\n" +
                "(arnold {name:'Arnold', _label:'user'}),\n" +
                "(charlie {name:'Charlie', _label:'user'}),\n" +
                "(gordon {name:'Gordon', _label:'user'}),\n" +
                "(lucy {name:'Lucy', _label:'user'}),\n" +
                "(emily {name:'Emily', _label:'user'}),\n" +
                "(sarah {name:'Sarah', _label:'user'}),\n" +
                "(kate {name:'Kate', _label:'user'}),\n" +
                "(mike {name:'Mike', _label:'user'}),\n" +
                "(paula {name:'Paula', _label:'user'}),\n" +
                "ben-[:FRIEND]->charlie,\n" +
                "charlie-[:FRIEND]->lucy,\n" +
                "lucy-[:FRIEND]->sarah,\n" +
                "sarah-[:FRIEND]->mike,\n" +
                "arnold-[:FRIEND]->gordon,\n" +
                "gordon-[:FRIEND]->emily,\n" +
                "emily-[:FRIEND]->kate,\n" +
                "kate-[:FRIEND]->paula";

        return createFromCypher( db,
                "Simple Social Network",
                cypher,
                IndexParam.indexParam( "user", "name" ) );
    }
}
