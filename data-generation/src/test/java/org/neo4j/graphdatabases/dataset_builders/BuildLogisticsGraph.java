package org.neo4j.graphdatabases.dataset_builders;

import static org.neo4j.neode.Range.exactly;
import static org.neo4j.neode.Range.minMax;
import static org.neo4j.neode.TargetNodesStrategy.create;
import static org.neo4j.neode.TargetNodesStrategy.getOrCreate;
import static org.neo4j.neode.properties.Property.indexableProperty;

import java.io.File;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Random;

import org.joda.time.Interval;
import org.junit.Ignore;
import org.junit.Test;
import org.neo4j.graphdatabases.Logistics;
import org.neo4j.graphdatabases.dataset_builders.helpers.SevenDays;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.PropertyContainer;
import org.neo4j.kernel.EmbeddedGraphDatabase;
import org.neo4j.kernel.impl.util.FileUtils;
import org.neo4j.neode.Dataset;
import org.neo4j.neode.DatasetManager;
import org.neo4j.neode.NodeCollection;
import org.neo4j.neode.NodeSpecification;
import org.neo4j.neode.logging.SysOutLog;
import org.neo4j.neode.properties.Property;
import org.neo4j.neode.statistics.AsciiDocFormatter;
import org.neo4j.neode.statistics.GraphStatistics;
import sun.plugin.dom.exception.InvalidStateException;

@Ignore
public class BuildLogisticsGraph
{
    @Test
    public void buildLogistics() throws Exception
    {
        File dir = new File( Logistics.STORE_DIR );
        FileUtils.deleteRecursively( dir );

        Map<String, String> params = new HashMap<String, String>();
        params.put( "dump_configuration", "true" );
        params.put( "cache_type", "gcr" );

        GraphDatabaseService db = new EmbeddedGraphDatabase( Logistics.STORE_DIR, params );
        DatasetManager dsm = new DatasetManager( db, SysOutLog.INSTANCE );

        NodeSpecification parcelCentreSpec = dsm.nodeSpecification( "parcel-centre",
                indexableProperty( "name", "location", "parcel-centre" ) );
        NodeSpecification deliveryBaseSpec = dsm.nodeSpecification( "delivery-base",
                indexableProperty( "name", "location", "delivery-base" ) );
        NodeSpecification deliveryAreaSpec = dsm.nodeSpecification( "delivery-area",
                indexableProperty( "name", "location", "delivery-area" ) );
        NodeSpecification deliverySegmentSpec = dsm.nodeSpecification( "delivery-segment",
                indexableProperty( "name", "location", "delivery-segment" ) );

        Property costProperty = new CostProperty();

        Dataset dataset = dsm.newDataset( Logistics.TITLE );

        NodeCollection parcelCentres = parcelCentreSpec.create( Logistics.NUMBER_OF_PARCEL_CENTRES ).update( dataset );

        NodeCollection deliveryBases = parcelCentres.createRelationshipsTo(
                getOrCreate( deliveryBaseSpec, 400 )
                        .numberOfTargetNodes( minMax(
                                Logistics.MIN_NUMBER_OF_DELIVERY_BASES_PER_PARCEL_CENTRE,
                                Logistics.MAX_NUMBER_OF_DELIVERY_BASES_PER_PARCEL_CENTRE ) )
                        .relationship( dsm.relationshipSpecification( "CONNECTED_TO",
                                new IntervalProperty( 2 ),
                                costProperty ) )
                        .relationshipConstraints( exactly( 2 ) ) )
                .update( dataset );

        NodeCollection deliveryAreas = deliveryBases.createRelationshipsTo(
                create( deliveryAreaSpec )
                        .numberOfTargetNodes( minMax(
                                Logistics.MIN_NUMBER_OF_DELIVERY_AREAS_PER_DELIVERY_BASE,
                                Logistics.MAX_NUMBER_OF_DELIVERY_AREAS_PER_DELIVERY_BASE ) )
                        .relationship( dsm.relationshipSpecification( "DELIVERY_ROUTE",
                                new IntervalProperty( 3 ),
                                costProperty ) )
                        .relationshipConstraints( exactly( 3 ) ) )
                .update( dataset );
        deliveryAreas.createRelationshipsTo(
                create( deliverySegmentSpec )
                        .numberOfTargetNodes( minMax(
                                Logistics.MIN_NUMBER_OF_DELIVERY_SEGMENTS_PER_DELIVERY_AREA,
                                Logistics.MAX_NUMBER_OF_DELIVERY_SEGMENTS_PER_DELIVERY_AREA ) )
                        .relationship( dsm.relationshipSpecification( "DELIVERY_ROUTE",
                                new IntervalProperty( 3 ),
                                costProperty ) )
                        .relationshipConstraints( exactly( 3 ) ) )
                .updateNoReturn( dataset, 1000 );


        //Link every parcel centre to every other parcel centre
//        parcelCentres.createRelationshipsTo( getExisting( parcelCentres )
//                .numberOfNodes( parcelCentres.size() )
//                .relationship( dsm.relationshipSpecification( "CONNECTED_TO",
//                        new IntervalProperty( 1 ), costProperty ) )
//                .relationshipConstraints( RelationshipUniqueness.SINGLE_DIRECTION ) )
//                .updateNoReturn( dataset );


        dataset.end();

        GraphStatistics.create( db, Logistics.TITLE )
                .describeTo( new AsciiDocFormatter( SysOutLog.INSTANCE ) );

        db.shutdown();

    }

    private static class IntervalProperty extends Property
    {
        private final int numberOfIntervals;
        private final SevenDays sevenDays = new SevenDays( Logistics.START_DATE );

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
                throw new InvalidStateException( String.format( "counter: %s, numberOfIntervals: %s, iteration: %s",
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
