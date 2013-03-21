package org.neo4j.graphdatabases.dataset_builders.properties;

import static org.neo4j.neode.Range.minMax;
import static org.neo4j.neode.probabilities.ProbabilityDistribution.normalDistribution;

import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.joda.time.Duration;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;
import org.neo4j.neode.probabilities.ProbabilityDistribution;

public class ProjectDuration
{
    private static final ProbabilityDistribution normalDistribution = normalDistribution();
    private static final DateTimeFormatter fmt = DateTimeFormat.forPattern( "dd-MM-yyyy" );

    private final Long startMs;
    private final Long endMs;

    public ProjectDuration( Long startMs, Long endMs )
    {
        this.endMs = endMs;
        this.startMs = startMs;
    }

    public Long getStartDateMs()
    {
        return startMs;
    }

    public Long getEndDateMs()
    {
        return endMs;
    }

    public ProjectDuration getSubDuration()
    {
        DateTime startDateTime = new DateTime( startMs, DateTimeZone.UTC );
        DateTime endDateTime = new DateTime( endMs, DateTimeZone.UTC );

        int durationInDays = (int) new Duration( startDateTime, endDateTime ).getStandardDays();
        int offsetDaysFromStart = normalDistribution.generateSingle( minMax( 0, (int) (durationInDays * 0.75) ) );
        int remainingDays = durationInDays - offsetDaysFromStart;
        int subDurationInDays = (int) ((remainingDays * 0.75) +
                (normalDistribution.generateSingle( minMax( 0, (int) (remainingDays * (0.25)) ) )));

        DateTime subDurationStartDateTime = startDateTime.plusDays( offsetDaysFromStart );
        DateTime subDurationEndDateTime = subDurationStartDateTime.plusDays( subDurationInDays );

        return new ProjectDuration( subDurationStartDateTime.getMillis(), subDurationEndDateTime.getMillis() );
    }

    public String toString()
    {
        return "Start: " + new DateTime( startMs, DateTimeZone.UTC ).toString( fmt )
                + ", End : " + new DateTime( endMs, DateTimeZone.UTC ).toString( fmt );
    }
}
