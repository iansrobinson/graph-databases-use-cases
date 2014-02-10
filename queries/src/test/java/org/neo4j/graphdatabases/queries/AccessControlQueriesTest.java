package org.neo4j.graphdatabases.queries;

import static java.util.Arrays.asList;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.neo4j.graphdatabases.queries.helpers.Db.createFromCypher;
import static org.neo4j.graphdatabases.queries.testing.IndexParam.indexParam;

import java.util.*;

import org.junit.*;
import org.junit.rules.TestName;
import org.neo4j.cypher.javacompat.ExecutionResult;
import org.neo4j.graphdatabases.queries.helpers.PrintingExecutionEngineWrapper;
import org.neo4j.graphdatabases.queries.traversals.IndexResources;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.helpers.collection.IteratorUtil;

public class AccessControlQueriesTest
{
    @Rule
    public TestName testName = new TestName();

    private GraphDatabaseService db;
    private AccessControlQueries queries;

    @Before
    public void init()
    {
        db = createDatabase();
        queries = new AccessControlQueries( new PrintingExecutionEngineWrapper( db,
                "access-control-revised", testName ) );
    }

    @After
    public void shutdown()
    {
        db.shutdown();
    }

    @Test
    public void allowedWithInheritTrueGivesAccessToSubcompaniesAndAccounts() throws Exception
    {
        // Ben is member of two groups, both of which have ALLOWED_INHERIT.
        // He should, therefore, see all results below the companies to which
        // these permissions are attached.

        // when
        ExecutionResult results = queries.findAccessibleResources( "Ben" );

        // then
        Iterator<Map<String, Object>> iterator = results.iterator();

        assertTrue( iterator.hasNext() );

        assertEquals( "Account-1", iterator.next().get( "account" ) );
        assertEquals( "Account-2", iterator.next().get( "account" ) );
        assertEquals( "Account-3", iterator.next().get( "account" ) );
        assertEquals( "Account-6", iterator.next().get( "account" ) );
        assertEquals( "Account-4", iterator.next().get( "account" ) );
        assertEquals( "Account-5", iterator.next().get( "account" ) );
        assertEquals( "Account-7", iterator.next().get( "account" ) );

        assertFalse( iterator.hasNext() );
    }

    @Test
    public void deniedExcludesCompanyFromPermissionsTree() throws Exception
    {
        // Sarah is a member of Group-2, which has DENIED on Skunkworx.
        // Therefore Account-7 should not appear in the results.
        // Group-2 also has ALLOWED_DO_NOT_INHERIT on Acme, so Spinoff accounts,
        // a child of Acme, should not be included in results

        // when
        ExecutionResult results = queries.findAccessibleResources( "Sarah" );

        // then
        Iterator<Map<String, Object>> iterator = results.iterator();

        assertTrue( iterator.hasNext() );

        assertEquals( "Account-4", iterator.next().get( "account" ) );
        assertEquals( "Account-5", iterator.next().get( "account" ) );
        assertEquals( "Account-1", iterator.next().get( "account" ) );
        assertEquals( "Account-2", iterator.next().get( "account" ) );
        assertEquals( "Account-3", iterator.next().get( "account" ) );

        assertFalse( iterator.hasNext() );
    }

    @Test
    public void deniedExcludesCompanyFromPermissionsTree2() throws Exception
    {
        // Liz is a member of Group-4, which has ALLOWED_INHERIT on BigCo.
        // However, she is also a member of Group-5, which has DENIED on AcquiredLtd.
        // This DENIED also debars Liz from Subsidiary and DevShop.
        // Liz's membership of Group-6, with its ALLOWED_DO_NOT_INHERIT on One-Man Shop
        // gives Liz access to One-Man Shop

        // when
        ExecutionResult results = queries.findAccessibleResources( "Liz" );

        // then
        Iterator<Map<String, Object>> iterator = results.iterator();

        assertTrue( iterator.hasNext() );

        assertEquals( "Account-8", iterator.next().get( "account" ) );
        assertEquals( "Account-10", iterator.next().get( "account" ) );

        assertFalse( iterator.hasNext() );
    }

    @Test
    public void shouldGetAccessibleCompaniesForAdmin() throws Exception
    {
        // Sarah is a member of groups that have ALLOWED_INHERIT, ALLOWED_DO_NOT_INHERIT and DENIED
        // This tests this combination (for a 2-layer organizational structure).

        // given
        ExecutionResult results = queries.findAccessibleCompanies( "Sarah" );

        // then
        Iterator<Map<String, Object>> iterator = results.iterator();

        assertTrue( iterator.hasNext() );

        assertEquals( "Startup", iterator.next().get( "company" ) );
        assertEquals( "Acme", iterator.next().get( "company" ) );

        assertFalse( iterator.hasNext() );
    }

    @Test
    public void shouldGetAccessibleCompaniesForAdminWhereNoAllowedInheritFalse() throws Exception
    {
        // Ben is a member of groups that have ALLOWED_INHERIT

        // given
        ExecutionResult results = queries.findAccessibleCompanies( "Ben" );

        // then
        Iterator<Map<String, Object>> iterator = results.iterator();

        assertTrue( iterator.hasNext() );

        assertEquals( "Acme", iterator.next().get( "company" ) );
        assertEquals( "Spinoff", iterator.next().get( "company" ) );
        assertEquals( "Startup", iterator.next().get( "company" ) );
        assertEquals( "Skunkworkz", iterator.next().get( "company" ) );

        assertFalse( iterator.hasNext() );
    }

    @Test
    public void moreComplexShouldGetAccessibleCompaniesForAdmin() throws Exception
    {
        // Liz has ALLOWED_INHERIT at the top of a 3-layer org structure,
        // DENIED at the next level, and ALLOWED_DO_NOT_INHERIT at the bottom layer.

        // given
        ExecutionResult results = queries.findAccessibleCompanies( "Liz" );

        // then
        Iterator<Map<String, Object>> iterator = results.iterator();

        assertTrue( iterator.hasNext() );

        assertEquals( "BigCompany", iterator.next().get( "company" ) );
        assertEquals( "One-ManShop", iterator.next().get( "company" ) );

        assertFalse( iterator.hasNext() );
    }

    @Test
    public void shouldFindAccessibleAccountsForAdminAndCompany() throws Exception
    {
        // given
        ExecutionResult results = queries.findAccessibleAccountsForCompany( "Sarah", "Startup" );

        // then
        Iterator<Map<String, Object>> iterator = results.iterator();

        assertTrue( iterator.hasNext() );

        assertEquals( "Account-4", iterator.next().get( "account" ) );
        assertEquals( "Account-5", iterator.next().get( "account" ) );

        assertFalse( iterator.hasNext() );

    }

    @Test
    public void moreComplexShouldFindAccessibleAccountsForAdminAndCompany() throws Exception
    {
        // given
        ExecutionResult results = queries.findAccessibleAccountsForCompany( "Liz", "BigCompany" );

        // then
        Iterator<Map<String, Object>> iterator = results.iterator();

        assertTrue( iterator.hasNext() );

        assertEquals( "Account-8", iterator.next().get( "account" ) );

        assertFalse( iterator.hasNext() );

    }

    @Test
    public void shouldFindAccessibleAccountsForAdminAndCompanyWhenNoAllowedWithInheritFalse() throws Exception
    {
        // given
        ExecutionResult results = queries.findAccessibleAccountsForCompany( "Ben", "Startup" );

        // then
        Iterator<Map<String, Object>> iterator = results.iterator();

        assertTrue( iterator.hasNext() );

        assertEquals( "Account-4", iterator.next().get( "account" ) );
        assertEquals( "Account-5", iterator.next().get( "account" ) );
        assertEquals( "Account-7", iterator.next().get( "account" ) );

        assertFalse( iterator.hasNext() );
    }

    @Test
    public void shouldFindAdminForAccountResourceWhereAllowedInheritAndAllowedNotInherit() throws Exception
    {
        // Account-10 is associated with One-ManShop
        // One-ManShop-CHILD_OF->Subsidiary-CHILD_OF->AcquiredLtd->CHILD_OF->BigCompany

        // Liz is a member of:
        //   Group-6, which has ALLOWED_DO_NOT_INHERIT to OneManShop
        //   Group-5, which has DENIED on AcquiredLtd
        //   Group-4, which has ALLOWED_INHERIT on BigCompany
        // She has access to Account-10 by virtue of Group-6

        // Phil is a member of:
        //   Group-7, which has ALLOWED_INHERIT on Subsidiary
        // He has access to Account-10 by virtue of Group-7

        // given
        ExecutionResult results = queries.findAdminForResource( "Account-10" );

        // then
        Iterator<Map<String, Object>> iterator = results.iterator();

        assertTrue( iterator.hasNext() );

        assertEquals( "Phil", iterator.next().get( "admin" ) );
        assertEquals( "Liz", iterator.next().get( "admin" ) );

        assertFalse( iterator.hasNext() );
    }

    @Test
    public void shouldFindAdminForEmployeeResourceWhereAllowedInheritAndDenied() throws Exception
    {
        // Kate works for Skunkworkz
        // Skunkworkz-CHILD_OF->Startup

        // Sarah is a member of:
        //  Group-2, which has DENIED on Skunkworkz
        //  Group-3, which has ALLOWED_INHERIT on Startup
        // She is denied access to Kate by virtue of Group-2

        // Ben is a member of:
        //  Group-3, which has ALLOWED_INHERIT on Startup
        //  Group-1, which has no access to Kate's company chain
        // He has access by to Kate virtue of Group-3

        // given
        ExecutionResult results = queries.findAdminForResource( "Kate" );

        // then
        Iterator<Map<String, Object>> iterator = results.iterator();

        assertTrue( iterator.hasNext() );

        assertEquals( "Ben", iterator.next().get( "admin" ) );

        assertFalse( iterator.hasNext() );
    }

    @Test
    public void shouldFindAdminForCompanyWithAllowedInherit() throws Exception
    {
        // BigCompany<-CHILD_OF-AcquiredLtd<-CHILD_OF-Subsidiary<-CHILD_OF-One-ManShop

        // Liz is a member of:
        //   Group-6, which has ALLOWED_DO_NOT_INHERIT to One-ManShop
        //   Group-5, which has DENIED on AcquiredLtd
        //   Group-4, which has ALLOWED_INHERIT on BigCompany
        // She has access to BigCompany by virtue of Group-4

        // Phil is a member of:
        //   Group-7, which has ALLOWED_INHERIT on Subsidiary
        // He does not have access to BigCompany

        // given
        ExecutionResult results = queries.findAdminForCompany( "BigCompany" );

        // then
        Iterator<Map<String, Object>> iterator = results.iterator();

        assertTrue( iterator.hasNext() );

        assertEquals( "Liz", iterator.next().get( "admin" ) );

        assertFalse( iterator.hasNext() );
    }

    @Test
    public void shouldFindAdminForCompanyWithDenied() throws Exception
    {
        // AcquiredLtd<-CHILD_OF-Subsidiary<-CHILD_OF-One-ManShop

        // Liz is a member of:
        //   Group-6, which has ALLOWED_DO_NOT_INHERIT to One-ManShop
        //   Group-5, which has DENIED on AcquiredLtd
        //   Group-4, which has ALLOWED_INHERIT on BigCompany
        // She is denied access to AcquiredLtd by virtue of Group-5

        // Phil is a member of:
        //   Group-7, which has ALLOWED_INHERIT on Subsidiary
        // He does not have access to AcquiredLtd

        // given
        ExecutionResult results = queries.findAdminForCompany( "AcquiredLtd" );

        // then
        Iterator<Map<String, Object>> iterator = results.iterator();

        assertFalse( iterator.hasNext() );
    }

    @Test
    public void shouldFindAdminForCompanyWithAllowedInheritAndAllowedDoNotInheritTooLowInTree() throws Exception
    {
        //Subsidiary<-CHILD_OF-One-ManShop

        // Liz is a member of:
        //   Group-6, which has ALLOWED_DO_NOT_INHERIT to One-ManShop
        //   Group-5, which has DENIED on AcquiredLtd
        //   Group-4, which has ALLOWED_INHERIT on BigCompany
        // She is denied access to Subsidiary by virtue of Group-5

        // Phil is a member of:
        //   Group-7, which has ALLOWED_INHERIT on Subsidiary
        // He has access to Subsidiary by virtue of Group-7

        // given
        ExecutionResult results = queries.findAdminForCompany( "Subsidiary" );

        // then
        Iterator<Map<String, Object>> iterator = results.iterator();

        assertTrue( iterator.hasNext() );

        assertEquals( "Phil", iterator.next().get( "admin" ) );

        assertFalse( iterator.hasNext() );
    }

    @Test
    public void shouldFindAdminForCompanyWithAllowedInheritAndAllowedAllowedDoNotInherit() throws Exception
    {
        //One-ManShop

        // Liz is a member of:
        //   Group-6, which has ALLOWED_DO_NOT_INHERIT to One-ManShop
        //   Group-5, which has DENIED on AcquiredLtd
        //   Group-4, which has ALLOWED_INHERIT on BigCompany
        // She has access to One-ManShop by virtue of Group-6

        // Phil is a member of:
        //   Group-7, which has ALLOWED_INHERIT on Subsidiary
        // He has access to One-ManShop by virtue of Group-7

        // given
        ExecutionResult results = queries.findAdminForCompany( "One-ManShop" );

        // then
        Iterator<Map<String, Object>> iterator = results.iterator();

        assertTrue( iterator.hasNext() );

        assertEquals( "Phil", iterator.next().get( "admin" ) );
        assertEquals( "Liz", iterator.next().get( "admin" ) );

        assertFalse( iterator.hasNext() );
    }

    @Test
    public void shouldDetermineWhetherAdminHasAccessToResource() throws Exception
    {
        Map<String, List<Long>> testData = new LinkedHashMap<>();
        testData.put( "Alistair", asList( 1L, 0L ) );
        testData.put( "Account-8", asList( 1L, 0L ) );
        testData.put( "Eve", asList( 0L ) );
        testData.put( "Account-9", asList( 0L ) );
        testData.put( "Mary", asList( 0L ) );
        testData.put( "Account-12", asList( 0L ) );
        testData.put( "Gary", asList( 0L ) );
        testData.put( "Account-11", asList( 0L ) );
        testData.put( "Bill", asList( 0L, 1L ) );
        testData.put( "Account-10", asList( 0L, 1L ) );

        for ( String resourceName : testData.keySet() )
        {
            List<Long> expectedResults = testData.get( resourceName );
            Iterator<Long> expectedResultsIterator = expectedResults.iterator();

            // given
            ExecutionResult results = queries.hasAccessToResource( "Liz", resourceName );

            // then
            Iterator<Map<String, Object>> iterator = results.iterator();

            assertTrue( iterator.hasNext() );
            assertEquals( expectedResultsIterator.next(), iterator.next().get( "accessCount" ) );
            if ( expectedResultsIterator.hasNext() )
            {
                assertEquals( expectedResultsIterator.next(), iterator.next().get( "accessCount" ) );
            }
            assertFalse( iterator.hasNext() );
        }

    }

    @Test
    @Ignore("does conceptually not work")
    public void shouldDetermineWhetherAdminHasAccessToIndexedResource() throws Exception
    {
        Map<String, Boolean> testData = new LinkedHashMap<>();
        testData.put( "Alistair", true );
        testData.put( "Account-8", true );
        testData.put( "Eve", false );
        testData.put( "Account-9", false );
        testData.put( "Mary", false );
        testData.put( "Account-12", false );
        testData.put( "Gary", false );
        testData.put( "Account-11", false );
        testData.put( "Bill", true );
        testData.put( "Account-10", true );

        for ( Map.Entry<String, Boolean> entry : testData.entrySet() )
        {
            // given
            ExecutionResult results = queries.hasAccessToIndexedResource( "Liz", entry.getKey() );
//            System.out.println(results.dumpToString());
            // then
            assertEquals( entry.getKey(), entry.getValue(), isAuthorized( results ) );
        }

    }

    private boolean isAuthorized( ExecutionResult result )
    {
        Iterator<Long> accessCountIterator = result.columnAs( "accessCount" );
        boolean isAuthorized = false;
        while ( accessCountIterator.hasNext() )
        {
            isAuthorized |= accessCountIterator.next() > 0L;
        }
        return isAuthorized;
    }


    private static GraphDatabaseService createDatabase()
    {
        String cypher = "CREATE\n" +
                "(ben:Administrator {name:'Ben'}),\n" +
                "(sarah:Administrator {name:'Sarah'}),\n" +
                "(liz:Administrator {name:'Liz'}),\n" +
                "(phil:Administrator {name:'Phil'}),\n" +
                "(arnold:Employee:Resource {name:'Arnold'}),\n" +
                "(charlie:Employee:Resource {name:'Charlie'}),\n" +
                "(gordon:Employee:Resource {name:'Gordon'}),\n" +
                "(lucy:Employee:Resource {name:'Lucy'}),\n" +
                "(emily:Employee:Resource {name:'Emily'}),\n" +
                "(kate:Employee:Resource {name:'Kate'}),\n" +
                "(alistair:Employee:Resource {name:'Alistair'}),\n" +
                "(eve:Employee:Resource {name:'Eve'}),\n" +
                "(bill:Employee:Resource {name:'Bill'}),\n" +
                "(gary:Employee:Resource {name:'Gary'}),\n" +
                "(mary:Employee:Resource {name:'Mary'}),\n" +
                "(group1:Group {name:'Group-1'}),\n" +
                "(group2:Group {name:'Group-2'}),\n" +
                "(group3:Group {name:'Group-3'}),\n" +
                "(group4:Group {name:'Group-4'}),\n" +
                "(group5:Group {name:'Group-5'}),\n" +
                "(group6:Group {name:'Group-6'}),\n" +
                "(group7:Group {name:'Group-7'}),\n" +
                "(acme:Company {name:'Acme'}),\n" +
                "(spinoff:Company {name:'Spinoff'}),\n" +
                "(startup:Company {name:'Startup'}),\n" +
                "(skunkworkz:Company {name:'Skunkworkz'}),\n" +
                "(bigco:Company {name:'BigCompany'}),\n" +
                "(acquired:Company {name:'AcquiredLtd'}),\n" +
                "(subsidiary:Company {name:'Subsidiary'}),\n" +
                "(devshop:Company {name:'DevShop'}),\n" +
                "(onemanshop:Company {name:'One-ManShop'}),\n" +
                "(account1:Account:Resource {name:'Account-1'}),\n" +
                "(account2:Account:Resource {name:'Account-2'}),\n" +
                "(account3:Account:Resource {name:'Account-3'}),\n" +
                "(account4:Account:Resource {name:'Account-4'}),\n" +
                "(account5:Account:Resource {name:'Account-5'}),\n" +
                "(account6:Account:Resource {name:'Account-6'}),\n" +
                "(account7:Account:Resource {name:'Account-7'}),\n" +
                "(account8:Account:Resource {name:'Account-8'}),\n" +
                "(account9:Account:Resource {name:'Account-9'}),\n" +
                "(account10:Account:Resource {name:'Account-10'}),\n" +
                "(account11:Account:Resource {name:'Account-11'}),\n" +
                "(account12:Account:Resource {name:'Account-12'}),\n" +
                "ben-[:MEMBER_OF]->group1,\n" +
                "ben-[:MEMBER_OF]->group3,\n" +
                "sarah-[:MEMBER_OF]->group2,\n" +
                "sarah-[:MEMBER_OF]->group3,\n" +
                "liz-[:MEMBER_OF]->group4,\n" +
                "liz-[:MEMBER_OF]->group5,\n" +
                "liz-[:MEMBER_OF]->group6,\n" +
                "phil-[:MEMBER_OF]->group7,\n" +
                "spinoff-[:CHILD_OF]->acme,\n" +
                "skunkworkz-[:CHILD_OF]->startup,\n" +
                "acquired-[:CHILD_OF]->bigco,\n" +
                "subsidiary-[:CHILD_OF]->acquired,\n" +
                "onemanshop-[:CHILD_OF]->subsidiary,\n" +
                "devshop-[:CHILD_OF]->subsidiary,\n" +
                "arnold-[:WORKS_FOR]->acme,\n" +
                "charlie-[:WORKS_FOR]->acme,\n" +
                "gordon-[:WORKS_FOR]->startup,\n" +
                "lucy-[:WORKS_FOR]->startup,\n" +
                "emily-[:WORKS_FOR]->spinoff,\n" +
                "kate-[:WORKS_FOR]->skunkworkz,\n" +
                "alistair-[:WORKS_FOR]->bigco,\n" +
                "eve-[:WORKS_FOR]->acquired,\n" +
                "gary-[:WORKS_FOR]->subsidiary,\n" +
                "mary-[:WORKS_FOR]->devshop,\n" +
                "bill-[:WORKS_FOR]->onemanshop,\n" +
                "arnold-[:HAS_ACCOUNT]->account1,\n" +
                "arnold-[:HAS_ACCOUNT]->account2,\n" +
                "charlie-[:HAS_ACCOUNT]->account3,\n" +
                "gordon-[:HAS_ACCOUNT]->account4,\n" +
                "lucy-[:HAS_ACCOUNT]->account5,\n" +
                "emily-[:HAS_ACCOUNT]->account6,\n" +
                "kate-[:HAS_ACCOUNT]->account7,\n" +
                "alistair-[:HAS_ACCOUNT]->account8,\n" +
                "eve-[:HAS_ACCOUNT]->account9,\n" +
                "bill-[:HAS_ACCOUNT]->account10,\n" +
                "gary-[:HAS_ACCOUNT]->account11,\n" +
                "mary-[:HAS_ACCOUNT]->account12,\n" +
                "group1-[:ALLOWED_INHERIT]->acme,\n" +
                "group2-[:ALLOWED_DO_NOT_INHERIT]->acme,\n" +
                "group2-[:DENIED]->skunkworkz,\n" +
                "group3-[:ALLOWED_INHERIT]->startup,\n" +
                "group4-[:ALLOWED_INHERIT]->bigco,\n" +
                "group5-[:DENIED]->acquired,\n" +
                "group6-[:ALLOWED_DO_NOT_INHERIT]->onemanshop,\n" +
                "group7-[:ALLOWED_INHERIT]->subsidiary";

        GraphDatabaseService graph = createFromCypher(
                "Access Control Revised",
                cypher,
                indexParam( "Administrator", "name" ),
                indexParam( "Employee", "name" ),
                indexParam( "Company", "name" ),
                indexParam( "Account", "name" ),
                indexParam( "Resource", "name" ));

        new IndexResources( graph ).execute();

        return graph;
    }
}
