package org.neo4j.graphdatabases.performance_tests;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Random;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import org.neo4j.graphdatabases.AccessControlWithRelationshipPropertiesConfig;
import org.neo4j.graphdatabases.performance_tests.testing.DefaultExecutionEngineWrapper;
import org.neo4j.graphdatabases.performance_tests.testing.MultipleTestRuns;
import org.neo4j.graphdatabases.performance_tests.testing.ParamsGenerator;
import org.neo4j.graphdatabases.performance_tests.testing.SingleTest;
import org.neo4j.graphdatabases.performance_tests.testing.SysOutWriter;
import org.neo4j.graphdatabases.queries.AccessControlWithRelationshipPropertiesQueries;
import org.neo4j.graphdatabases.queries.helpers.DbUtils;
import org.neo4j.graphdatabases.queries.helpers.QueryUnionExecutionResult;
import org.neo4j.graphdatabases.queries.testing.TestOutputWriter;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;

import static org.neo4j.graphdatabases.performance_tests.testing.PrintTestResults.printResults;

public class AccessControlWithRelationshipProperties
{
    private static final int NUMBER_OF_TEST_RUNS = 20;
    private static final int NUMBER_OF_RESULTS = 15;

    private static int numberOfAccounts;
    private static int numberOfCustomers;

    private static GraphDatabaseService db;
    private static AccessControlWithRelationshipPropertiesQueries queries;
    private static MultipleTestRuns multipleTestRuns;
    private static Random random;
    private static TestOutputWriter writer = SysOutWriter.INSTANCE;

    @BeforeClass
    public static void init()
    {
        db = DbUtils.existingDB( AccessControlWithRelationshipPropertiesConfig.STORE_DIR );

        queries = new AccessControlWithRelationshipPropertiesQueries( new DefaultExecutionEngineWrapper( db ) );
        multipleTestRuns = new MultipleTestRuns( NUMBER_OF_TEST_RUNS, writer );

        random = new Random();

        numberOfAccounts = DbUtils.numberOfItemsWithLabel(db, "Account");
        numberOfCustomers = DbUtils.numberOfItemsWithLabel(db, "Employee");

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
                    public QueryUnionExecutionResult execute( Map<String, String> params )
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
                    public QueryUnionExecutionResult execute( Map<String, String> params )
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
                    public QueryUnionExecutionResult execute( Map<String, String> params )
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
                    public QueryUnionExecutionResult execute( Map<String, String> params )
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
                    public QueryUnionExecutionResult execute( Map<String, String> params )
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
                    public QueryUnionExecutionResult execute( Map<String, String> params )
                    {
                        return queries.hasAccessToIndexedResource( params.get( "admin" ),
                                params.get( "resource" ) );
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
                    public QueryUnionExecutionResult execute( Map<String, String> params )
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
                    public QueryUnionExecutionResult execute( Map<String, String> params )
                    {
                        return queries.hasAccessToIndexedResource( params.get( "admin" ),
                                params.get( "resource" ) );
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

                String adminName = String.format( "Administrator-%s",
                        random.nextInt( AccessControlWithRelationshipPropertiesConfig.NUMBER_OF_ADMINS ) + 1 );

                String resourceName;
                if ( random.nextInt( 2 ) < 1 )
                {
                    resourceName = String.format( "Account-%s", random.nextInt( numberOfAccounts ) + 1 );
                }
                else
                {
                    resourceName = String.format( "Customer-%s", random.nextInt( numberOfCustomers ) + 1 );
                }

                QueryUnionExecutionResult result = queries.findAccessibleCompanies( adminName );
                Iterator<Map<String, Object>> iterator = result.iterator();
                String companyName = ((Node) iterator.next().get( "company" )).getProperty( "name" ).toString();

                params.put( "admin", adminName );
                params.put( "company", companyName );
                params.put( "resource", resourceName );

                return params;
            }
        };
    }

}
