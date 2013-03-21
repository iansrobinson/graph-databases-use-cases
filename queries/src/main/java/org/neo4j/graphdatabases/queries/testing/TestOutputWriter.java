/*
 * Copyright (C) 2012 Neo Technology
 * All rights reserved
 */
package org.neo4j.graphdatabases.queries.testing;

public interface TestOutputWriter
{
    void begin();
    void write(String value);
    void writeln( String value );
    void flush();
    void end();
}
