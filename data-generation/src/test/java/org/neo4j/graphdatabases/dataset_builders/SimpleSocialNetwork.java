package org.neo4j.graphdatabases.dataset_builders;

import java.io.File;

import org.junit.Test;

import org.neo4j.graphdatabases.SimpleSocialNetworkConfig;
import org.neo4j.graphdatabases.queries.helpers.DbUtils;
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

import static org.neo4j.neode.Range.minMax;
import static org.neo4j.neode.RelationshipUniqueness.BOTH_DIRECTIONS;
import static org.neo4j.neode.TargetNodesStrategy.getExisting;
import static org.neo4j.neode.properties.Property.indexableProperty;

public class SimpleSocialNetwork
{
    @Test
    public void buildSocialNetwork() throws Exception
    {
        File dir = new File( SimpleSocialNetworkConfig.STORE_DIR );
        FileUtils.deleteRecursively( dir );

        GraphDatabaseService db = new GraphDatabaseFactory()
                .newEmbeddedDatabaseBuilder( SimpleSocialNetworkConfig.STORE_DIR )
                .setConfig( DbUtils.dbConfig() )
                .newGraphDatabase();
        createSampleDataset( db );

        GraphStatistics.create( db, SimpleSocialNetworkConfig.TITLE )
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
                System.out.println( value );
            }
        } );

        NodeSpecification userSpec = dsm.nodeSpecification(
                "User", indexableProperty( db, "User", "name" ) );
        RelationshipSpecification friend =
                dsm.relationshipSpecification( "FRIEND" );

        Dataset dataset =
                dsm.newDataset( "Simple social network example" );

        NodeCollection users =
                userSpec.create( SimpleSocialNetworkConfig.NUMBER_USERS )
                        .update( dataset );

        users.createRelationshipsTo(
                getExisting( users )
                        .numberOfTargetNodes( minMax( SimpleSocialNetworkConfig.MIN_NUMBER_OF_FRIENDS,
                                SimpleSocialNetworkConfig.MAX_NUMBER_OF_FRIENDS ) )
                        .relationship( friend )
                        .relationshipConstraints( BOTH_DIRECTIONS ) )
                .updateNoReturn( dataset, 20000 );

        dataset.end();
    }
}
