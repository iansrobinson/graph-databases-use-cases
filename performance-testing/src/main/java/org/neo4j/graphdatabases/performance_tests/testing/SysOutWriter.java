package org.neo4j.graphdatabases.performance_tests.testing;

import org.neo4j.graphdatabases.queries.testing.TestOutputWriter;

public class SysOutWriter implements TestOutputWriter
{
    public static final TestOutputWriter INSTANCE = new SysOutWriter();

    private SysOutWriter(){}

    @Override
    public void begin()
    {
    }

    @Override
    public void write( String value )
    {
        System.out.print(value);
    }

    @Override
    public void writeln( String value )
    {
        System.out.println( value );
    }

    @Override
    public void flush()
    {
    }

    @Override
    public void end()
    {
    }
}
