package org.neo4j.graphdatabases.dataset_builders;

import static org.neo4j.neode.Range.minMax;
import static org.neo4j.neode.RelationshipUniqueness.BOTH_DIRECTIONS;
import static org.neo4j.neode.TargetNodesStrategy.getExisting;
import static org.neo4j.neode.properties.Property.indexableProperty;

import java.io.File;
import java.util.HashMap;
import java.util.Map;

import org.junit.Ignore;
import org.junit.Test;
import org.neo4j.graphdatabases.SimpleSocialNetwork;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.factory.GraphDatabaseFactory;
import org.neo4j.kernel.impl.util.FileUtils;
import org.neo4j.neode.Dataset;
import org.neo4j.neode.DatasetManager;
import org.neo4j.neode.NodeCollection;
import org.neo4j.neode.NodeSpecification;
import org.neo4j.neode.RelationshipSpecification;
import org.neo4j.neode.logging.Log;
import org.neo4j.neode.logging.SysOutLog;
import org.neo4j.neode.statistics.AsciiDocFormatter;
import org.neo4j.neode.statistics.GraphStatistics;

@Ignore
public class BuildSimpleSocialNetworkGraph
{
    @Test
    public void buildSocialNetwork() throws Exception
    {
        File dir = new File( SimpleSocialNetwork.STORE_DIR );
        FileUtils.deleteRecursively( dir );

        Map<String, String> params = new HashMap<String, String>();
        params.put( "dump_configuration", "true" );
        params.put( "cache_type", "gcr" );

        GraphDatabaseService db = new GraphDatabaseFactory()
                            .newEmbeddedDatabaseBuilder( SimpleSocialNetwork.STORE_DIR )
                            .setConfig( params )
                            .newGraphDatabase();
        createSampleDataset( db );

        GraphStatistics.create( db, SimpleSocialNetwork.TITLE )
                .describeTo( new AsciiDocFormatter( SysOutLog.INSTANCE ) );

        db.shutdown();
    }

    private void createSampleDataset( GraphDatabaseService db )
    {
        DatasetManager dsm = new DatasetManager( db, new Log()
        {
            @Override
            public void write( String value )
            {
                System.out.println(value);
            }
        } );

        NodeSpecification userSpec = dsm.nodeSpecification(
                "user", indexableProperty( "name" ) );
        RelationshipSpecification friend =
                dsm.relationshipSpecification( "FRIEND" );

        Dataset dataset =
                dsm.newDataset( "Simple social network example" );

        NodeCollection users =
                userSpec.create( SimpleSocialNetwork.NUMBER_USERS )
                        .update( dataset );

        users.createRelationshipsTo(
                getExisting( users )
                        .numberOfTargetNodes( minMax( SimpleSocialNetwork.MIN_NUMBER_OF_FRIENDS,
                                SimpleSocialNetwork.MAX_NUMBER_OF_FRIENDS ) )
                        .relationship( friend )
                        .relationshipConstraints( BOTH_DIRECTIONS ) )
                .updateNoReturn( dataset, 20000 );

        dataset.end();
    }
}
