/*
 * Copyright (C) 2012 Neo Technology
 * All rights reserved
 */
package org.neo4j.graphdatabases.performance_tests.testing;

import org.neo4j.graphdatabases.queries.testing.TestOutputWriter;

public interface SingleTestRunResultHandler
{
    void handle( String queryType, String formattedResult );
    void summarize( TestOutputWriter writer );
}
