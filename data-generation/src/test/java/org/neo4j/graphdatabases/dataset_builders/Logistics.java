package org.neo4j.graphdatabases.dataset_builders;

import java.io.File;
import java.util.Iterator;
import java.util.NoSuchElementException;
import java.util.Random;

import org.joda.time.Interval;
import org.junit.Test;

import org.neo4j.graphdatabases.LogisticsConfig;
import org.neo4j.graphdatabases.dataset_builders.helpers.SevenDays;
import org.neo4j.graphdatabases.queries.helpers.DbUtils;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.PropertyContainer;
import org.neo4j.graphdb.factory.GraphDatabaseFactory;
import org.neo4j.kernel.impl.util.FileUtils;
import org.neo4j.neode.Dataset;
import org.neo4j.neode.DatasetManager;
import org.neo4j.neode.NodeCollection;
import org.neo4j.neode.NodeSpecification;
import org.neo4j.neode.logging.SysOutLog;
import org.neo4j.neode.properties.Property;
import org.neo4j.neode.statistics.AsciiDocFormatter;
import org.neo4j.neode.statistics.GraphStatistics;

import static org.neo4j.neode.Range.exactly;
import static org.neo4j.neode.Range.minMax;
import static org.neo4j.neode.TargetNodesStrategy.create;
import static org.neo4j.neode.TargetNodesStrategy.getOrCreate;
import static org.neo4j.neode.properties.Property.indexableProperty;

public class Logistics
{
    @Test
    public void buildLogistics() throws Exception
    {
        File dir = new File( LogisticsConfig.STORE_DIR );
        FileUtils.deleteRecursively( dir );

        GraphDatabaseService db = new GraphDatabaseFactory()
                .newEmbeddedDatabaseBuilder( LogisticsConfig.STORE_DIR )
                .setConfig( DbUtils.dbConfig() )
                .newGraphDatabase();
        DatasetManager dsm = new DatasetManager( db, SysOutLog.INSTANCE );

        NodeSpecification parcelCentreSpec = dsm.nodeSpecification( "ParcelCentre",
                indexableProperty( db, "ParcelCentre", "name", "Location" ) );
        NodeSpecification deliveryBaseSpec = dsm.nodeSpecification( "DeliveryBase",
                indexableProperty( db, "DeliveryBase","name", "Location" ) );
        NodeSpecification deliveryAreaSpec = dsm.nodeSpecification( "DeliveryArea",
                indexableProperty( db, "DeliveryArea", "name", "Location" ) );
        NodeSpecification deliverySegmentSpec = dsm.nodeSpecification( "DeliverySegment",
                indexableProperty( db, "DeliverySegment", "name", "Location" ) );

        Property costProperty = new CostProperty();

        Dataset dataset = dsm.newDataset( LogisticsConfig.TITLE );

        NodeCollection parcelCentres = parcelCentreSpec.create( LogisticsConfig.NUMBER_OF_PARCEL_CENTRES ).update(
                dataset );

        NodeCollection deliveryBases = parcelCentres.createRelationshipsTo(
                getOrCreate( deliveryBaseSpec, 400 )
                        .numberOfTargetNodes( minMax(
                                LogisticsConfig.MIN_NUMBER_OF_DELIVERY_BASES_PER_PARCEL_CENTRE,
                                LogisticsConfig.MAX_NUMBER_OF_DELIVERY_BASES_PER_PARCEL_CENTRE ) )
                        .relationship( dsm.relationshipSpecification( "CONNECTED_TO",
                                new IntervalProperty( 2 ),
                                costProperty ) )
                        .relationshipConstraints( exactly( 2 ) ) )
                .update( dataset );

        NodeCollection deliveryAreas = deliveryBases.createRelationshipsTo(
                create( deliveryAreaSpec )
                        .numberOfTargetNodes( minMax(
                                LogisticsConfig.MIN_NUMBER_OF_DELIVERY_AREAS_PER_DELIVERY_BASE,
                                LogisticsConfig.MAX_NUMBER_OF_DELIVERY_AREAS_PER_DELIVERY_BASE ) )
                        .relationship( dsm.relationshipSpecification( "DELIVERY_ROUTE",
                                new IntervalProperty( 3 ),
                                costProperty ) )
                        .relationshipConstraints( exactly( 3 ) ) )
                .update( dataset );
        deliveryAreas.createRelationshipsTo(
                create( deliverySegmentSpec )
                        .numberOfTargetNodes( minMax(
                                LogisticsConfig.MIN_NUMBER_OF_DELIVERY_SEGMENTS_PER_DELIVERY_AREA,
                                LogisticsConfig.MAX_NUMBER_OF_DELIVERY_SEGMENTS_PER_DELIVERY_AREA ) )
                        .relationship( dsm.relationshipSpecification( "DELIVERY_ROUTE",
                                new IntervalProperty( 3 ),
                                costProperty ) )
                        .relationshipConstraints( exactly( 3 ) ) )
                .updateNoReturn( dataset, 1000 );

        dataset.end();

        GraphStatistics.create( db, LogisticsConfig.TITLE )
                .describeTo( new AsciiDocFormatter( SysOutLog.INSTANCE ) );

        db.shutdown();

    }

    private static class IntervalProperty extends Property
    {
        private final int numberOfIntervals;
        private final SevenDays sevenDays = new SevenDays( LogisticsConfig.START_DATE );

        private Iterator<Interval> intervals;
        private int counter;

        private IntervalProperty( int numberOfIntervals )
        {
            this.numberOfIntervals = numberOfIntervals;
            this.counter = numberOfIntervals;
        }

        @Override
        public void setProperty( PropertyContainer propertyContainer, GraphDatabaseService graphDatabaseService,
                                 String label, int iteration )
        {
            if ( counter++ >= numberOfIntervals )
            {
                intervals = sevenDays.calculateIntervals( numberOfIntervals ).iterator();
                counter = 1;
            }

            try
            {
                Interval nextInterval = intervals.next();
                propertyContainer.setProperty( "start_date", nextInterval.getStartMillis() );
                propertyContainer.setProperty( "end_date", nextInterval.getEndMillis() );
            }
            catch ( NoSuchElementException e )
            {
                throw new IllegalStateException( String.format( "counter: %s, numberOfIntervals: %s, iteration: %s",
                        counter, numberOfIntervals, iteration ) );
            }
        }
    }

    private static class CostProperty extends Property
    {
        private final Random random = new Random();

        @Override
        public void setProperty( PropertyContainer propertyContainer, GraphDatabaseService graphDatabaseService,
                                 String s, int i )
        {
            propertyContainer.setProperty( "cost", random.nextInt( 10 ) + 1 );
        }
    }
}
