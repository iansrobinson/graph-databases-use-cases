package org.neo4j.graphdatabases.dataset_builders.helpers;

import static java.util.Arrays.asList;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Random;

import org.joda.time.DateTime;
import org.joda.time.Interval;

public class SevenDays
{
    private final DateTime start;
    private final Random random;

    @SuppressWarnings( "unchecked" )
    private static final List<List<Integer>> days = asList(
            asList( 1, 1, 5 ),
            asList( 1, 2, 4 ),
            asList( 1, 3, 3 ),
            asList( 2, 2, 3 ) );

    public SevenDays( DateTime start )
    {
        this.start = start;
        this.random = new Random();
    }

    public Iterable<Interval> calculateIntervals( int numberOfIntervals )
    {
        if ( numberOfIntervals < 1 || numberOfIntervals > 3 )
        {
            throw new IllegalArgumentException( "numberOfIntervals must be 1 or 3" );
        }

        List<Interval> intervals = new ArrayList<Interval>();

        if ( numberOfIntervals == 1 )
        {
            intervals.add( new Interval( start, start.plusDays( 7 ) ) );
        }
        else if ( numberOfIntervals == 2 )
        {
            int numberOfDays = random.nextInt( 6 ) + 1;
            DateTime mid = start.plusDays( numberOfDays );
            intervals.add( new Interval( start, mid ) );
            intervals.add( new Interval( mid, start.plusDays( 7 ) ) );
        }
        else
        {
            int i = random.nextInt( days.size() );
            List<Integer> plusDays = days.get( i );
            Collections.shuffle( plusDays, random );

            DateTime mid1 = start.plusDays( plusDays.get( 0 ) );
            DateTime mid2 = mid1.plusDays( plusDays.get( 1 ) );

            intervals.add( new Interval( start, mid1 ) );
            intervals.add( new Interval( mid1, mid2 ) );
            intervals.add( new Interval( mid2, start.plusDays( 7 ) ) );
        }

        return intervals;
    }

}
