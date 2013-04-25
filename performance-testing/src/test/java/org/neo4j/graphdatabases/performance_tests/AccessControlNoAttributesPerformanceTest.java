package org.neo4j.graphdatabases.performance_tests;

import static org.neo4j.graphdatabases.performance_tests.testing.PrintTestResults.printResults;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Random;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;
import org.neo4j.cypher.javacompat.ExecutionResult;
import org.neo4j.graphdatabases.AccessControl;
import org.neo4j.graphdatabases.AccessControlNoAttributes;
import org.neo4j.graphdatabases.performance_tests.testing.DefaultExecutionEngineWrapper;
import org.neo4j.graphdatabases.performance_tests.testing.MultipleTestRuns;
import org.neo4j.graphdatabases.performance_tests.testing.ParamsGenerator;
import org.neo4j.graphdatabases.performance_tests.testing.SingleTest;
import org.neo4j.graphdatabases.performance_tests.testing.SysOutWriter;
import org.neo4j.graphdatabases.queries.AccessControlNoAttributesQueries;
import org.neo4j.graphdatabases.queries.helpers.DbUtils;
import org.neo4j.graphdatabases.queries.testing.TestOutputWriter;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.factory.GraphDatabaseFactory;

@Ignore
public class AccessControlNoAttributesPerformanceTest
{
    private static final int NUMBER_OF_TEST_RUNS = 100;
    private static final int NUMBER_OF_RESULTS = 15;

    private static int numberOfAccounts;
    private static int numberOfEmployees;

    private static GraphDatabaseService db;
    private static AccessControlNoAttributesQueries queries;
    private static MultipleTestRuns multipleTestRuns;
    private static Random random;
    private static TestOutputWriter writer = SysOutWriter.INSTANCE;

    @BeforeClass
    public static void init()
    {
        try
        {
            Map<String, String> params = new HashMap<String, String>();
            params.put( "dump_configuration", "true" );
            params.put( "cache_type", "gcr" );
            params.put( "allow_store_upgrade", "true" );

            db = new GraphDatabaseFactory()
                    .newEmbeddedDatabaseBuilder( AccessControlNoAttributes.STORE_DIR )
                    .setConfig( params )
                    .newGraphDatabase();
            queries = new AccessControlNoAttributesQueries( new DefaultExecutionEngineWrapper( db ) );
            multipleTestRuns = new MultipleTestRuns( NUMBER_OF_TEST_RUNS, writer );

            random = new Random();

            numberOfAccounts = DbUtils.numberOfItemsInIndex( db, "account", "name" );
            numberOfEmployees = DbUtils.numberOfItemsInIndex( db, "employee", "name" );

//        GraphStatistics.create( db, AccessControlNoAttributes.TITLE )
//                .describeTo( new AsciiDocFormatter( SysOutLog.INSTANCE ) );
        }
        catch ( Exception ex )
        {
            System.out.println( ex.getMessage() );
        }
    }

    @AfterClass
    public static void teardown()
    {
        db.shutdown();
    }

    @Test
    public void findAccessibleResources() throws Exception
    {
        // when
        multipleTestRuns.execute( "Find accessible resources for admin",
                createParams(),
                printResults( NUMBER_OF_RESULTS, writer ),
                new SingleTest()
                {
                    @Override
                    public String queryType()
                    {
                        return "Cypher";
                    }

                    @Override
                    public ExecutionResult execute( Map<String, String> params )
                    {
                        return queries.findAccessibleResources( params.get( "admin" ) );
                    }
                } );
    }

    @Test
    public void findAccessibleCompanies() throws Exception
    {
        // when
        multipleTestRuns.execute( "Find accessible companies for admin",
                createParams(),
                printResults( NUMBER_OF_RESULTS, writer ),
                new SingleTest()
                {
                    @Override
                    public String queryType()
                    {
                        return "Cypher";
                    }

                    @Override
                    public ExecutionResult execute( Map<String, String> params )
                    {
                        return queries.findAccessibleCompanies( params.get( "admin" ) );
                    }
                } );
    }

    @Test
    public void findAccessibleAccountsForCompany() throws Exception
    {
        // when
        multipleTestRuns.execute( "Find accessible accounts for company for admin",
                createParams(),
                printResults( NUMBER_OF_RESULTS, writer ),
                new SingleTest()
                {
                    @Override
                    public String queryType()
                    {
                        return "Cypher";
                    }

                    @Override
                    public ExecutionResult execute( Map<String, String> params )
                    {
                        return queries.findAccessibleAccountsForCompany(
                                params.get( "admin" ),
                                params.get( "company" ) );
                    }
                } );
    }

    @Test
    public void findAdminForCompany() throws Exception
    {
        // when
        multipleTestRuns.execute( "Find admins for company",
                createParams(),
                printResults( NUMBER_OF_RESULTS, writer ),
                new SingleTest()
                {
                    @Override
                    public String queryType()
                    {
                        return "Cypher";
                    }

                    @Override
                    public ExecutionResult execute( Map<String, String> params )
                    {
                        return queries.findAdminForCompany( params.get( "company" ) );
                    }
                } );
    }

    @Test
    public void findAdminForResource() throws Exception
    {
        // when
        multipleTestRuns.execute( "Find admins for resource",
                createParams(),
                printResults( NUMBER_OF_RESULTS, writer ),
                new SingleTest()
                {
                    @Override
                    public String queryType()
                    {
                        return "Cypher";
                    }

                    @Override
                    public ExecutionResult execute( Map<String, String> params )
                    {
                        return queries.findAdminForResource( params.get( "resource" ) );
                    }
                }
        );
    }

    @Test
    public void hasAccessToResource() throws Exception
    {
        // when
        multipleTestRuns.execute( "Does admin have access to resource?",
                createParams(),
                printResults( NUMBER_OF_RESULTS, writer ),
                new SingleTest()
                {
                    @Override
                    public String queryType()
                    {
                        return "Cypher (indexed resources)";
                    }

                    @Override
                    public ExecutionResult execute( Map<String, String> params )
                    {
                        return queries.hasAccessToIndexedResource( params.get( "admin" ), params.get( "resource" ) );
                    }
                }
        );
    }

    @Test
    public void hasAccessToResourceBakeoff() throws Exception
    {
        // when
        multipleTestRuns.execute( "Does admin have access to resource?",
                createParams(),
                printResults( NUMBER_OF_RESULTS, writer ),
                new SingleTest()
                {
                    @Override
                    public String queryType()
                    {
                        return "Cypher";
                    }

                    @Override
                    public ExecutionResult execute( Map<String, String> params )
                    {
                        return queries.hasAccessToResource( params.get( "admin" ), params.get( "resource" ) );
                    }
                }, new SingleTest()
                {
                    @Override
                    public String queryType()
                    {
                        return "Cypher (indexed resources)";
                    }

                    @Override
                    public ExecutionResult execute( Map<String, String> params )
                    {
                        return queries.hasAccessToIndexedResource( params.get( "admin" ), params.get( "resource" ) );
                    }
                }
        );
    }

    private ParamsGenerator createParams()
    {
        return new ParamsGenerator()
        {
            @Override
            public Map<String, String> generateParams()
            {
                HashMap<String, String> params = new HashMap<String, String>();

                String adminName = String.format( "administrator-%s",
                        random.nextInt( AccessControl.NUMBER_OF_ADMINS ) + 1 );

                String resourceName;
                if ( random.nextInt( 2 ) < 1 )
                {
                    resourceName = String.format( "account-%s", random.nextInt( numberOfAccounts ) + 1 );
                }
                else
                {
                    resourceName = String.format( "customer-%s", random.nextInt( numberOfEmployees ) + 1 );
                }

                ExecutionResult result = queries.findAccessibleCompanies( adminName );
                Iterator<Map<String, Object>> iterator = result.iterator();
                String companyName = (String) iterator.next().get( "company" );

                params.put( "admin", adminName );
                params.put( "company", companyName );
                params.put( "resource", resourceName );

                return params;
            }
        };
    }


}

