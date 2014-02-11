package org.neo4j.graphdatabases.queries;

import java.util.HashMap;
import java.util.Map;

import org.neo4j.graphdatabases.queries.helpers.ExecutionEngineWrapper;
import org.neo4j.graphdatabases.queries.helpers.QueryUnionExecutionEngine;
import org.neo4j.graphdatabases.queries.helpers.QueryUnionExecutionResult;

public class AccessControlWithRelationshipPropertiesQueries
{
    private final QueryUnionExecutionEngine executionEngine;

    public AccessControlWithRelationshipPropertiesQueries( ExecutionEngineWrapper executionEngine )
    {
        this.executionEngine = new QueryUnionExecutionEngine( executionEngine );
    }

    public QueryUnionExecutionResult findAccessibleResources( String adminName )
    {
        String inheritedQuery = "MATCH (admin:Administrator {name:{adminName}})\n" +
                "MATCH paths=(admin)-[:MEMBER_OF]->()-[permission:ALLOWED]->()<-[:CHILD_OF*0..3]-()" +
                "<-[:WORKS_FOR]-(employee)-[:HAS_ACCOUNT]->(account)\n" +
                "WHERE (permission.inherit=true) AND NOT (admin-[:MEMBER_OF]->()-[:DENIED]->()<-[:CHILD_OF*0..3]-()" +
                "<-[:WORKS_FOR]-employee-[:HAS_ACCOUNT]->account)\n" +
                "RETURN paths";
        String notInheritedQuery = "MATCH (admin:Administrator {name:{adminName}})\n" +
                "MATCH paths=(admin)-[:MEMBER_OF]->()-[permission:ALLOWED]->()" +
                "<-[:WORKS_FOR]-employee-[:HAS_ACCOUNT]->account\n" +
                "WHERE (permission.inherit=false)\n" +
                "RETURN paths";

        Map<String, Object> params = new HashMap<>();
        params.put( "adminName", adminName );

        return executionEngine.execute( params, inheritedQuery, notInheritedQuery );

    }
    // todo no result for query1 ?
    public QueryUnionExecutionResult findAccessibleCompanies( String adminName )
    {
        String inheritedQuery = "MATCH (admin:Administrator {name:{adminName}})\n" +
                "MATCH admin-[:MEMBER_OF]->()-[permission:ALLOWED]->()<-[:CHILD_OF*0..3]-company\n" +
                "WHERE (permission.inherit=true) AND NOT (admin-[:MEMBER_OF]->()-[:DENIED]->()<-[:CHILD_OF*0." +
                ".3]-company)\n" +
                "RETURN company";

        String notInheritedQuery = "MATCH (admin:Administrator {name:{adminName}})\n" +
                "MATCH admin-[:MEMBER_OF]->()-[permission:ALLOWED]->(company)\n" +
                "WHERE (permission.inherit=false)\n" +
                "RETURN company";

        Map<String, Object> params = new HashMap<>();
        params.put( "adminName", adminName );

        return executionEngine.execute( params, inheritedQuery, notInheritedQuery );
    }

    public QueryUnionExecutionResult findAccessibleAccountsForCompany( String adminName,
                                                                           String companyName )
    {
        String inheritedQuery = "MATCH (admin:Administrator {name:{adminName}}),\n" +
                "                      (company:Company{name:{companyName}})\n" +
                "MATCH admin-[:MEMBER_OF]->group-[permission:ALLOWED]->company<-[:CHILD_OF*0." +
                ".3]-subcompany<-[:WORKS_FOR]-employee-[:HAS_ACCOUNT]->account\n" +
                "WHERE (permission.inherit=true) AND NOT (admin-[:MEMBER_OF]->()-[:DENIED]->()<-[:CHILD_OF*0." +
                ".3]-subcompany)\n" +
                "RETURN account";

        String notInheritedQuery = "MATCH (admin:Administrator {name:{adminName}}),\n" +
                "                         (company:Company{name:{companyName}})\n" +
                "MATCH admin-[:MEMBER_OF]->group-[permission:ALLOWED]->company<-[:WORKS_FOR]-employee-[:HAS_ACCOUNT" +
                "]->account\n" +
                "WHERE (permission.inherit=false)\n" +
                "RETURN account";

        Map<String, Object> params = new HashMap<>();
        params.put( "adminName", adminName );
        params.put( "companyName", companyName );

        return executionEngine.execute( params, inheritedQuery, notInheritedQuery );
    }

    // todo no result for query2
    public QueryUnionExecutionResult findAdminForResource( String resourceName )
    {
        String inheritedQuery = "MATCH (resource:Resource {name:{resourceName}}) \n" +
                        "MATCH p=resource-[:WORKS_FOR|HAS_ACCOUNT*1..2]-company-[:CHILD_OF*0..3]->()<-[permission:ALLOWED]-()" +
                        "<-[:MEMBER_OF]-admin\n" +
                        "WHERE (permission.inherit=true) AND NOT (admin-[:MEMBER_OF]->()-[:DENIED]->()<-[:CHILD_OF*0..3]-company)\n" +
                        "RETURN DISTINCT admin, p";

                String notInheritedQuery = "MATCH (resource:Resource{name:{resourceName}})\n" +
                        "MATCH p=resource-[:WORKS_FOR|HAS_ACCOUNT*1..2]-company<-[permission:ALLOWED]-()" +
                        "<-[:MEMBER_OF]-admin\n" +
                        "WHERE (permission.inherit=false)\n" +
                        "RETURN DISTINCT admin, p";

                Map<String, Object> params = new HashMap<>();
                params.put( "resourceName", resourceName );

                return executionEngine.execute( params, inheritedQuery, notInheritedQuery );
    }

    public QueryUnionExecutionResult findAdminForCompany( String companyName )
    {
        String inheritedQuery = "MATCH (company:Company{name:{companyName}})\n" +
                        "MATCH p=company-[:CHILD_OF*0..3]->()<-[permission:ALLOWED]-()<-[:MEMBER_OF]-admin\n" +
                        "WHERE (permission.inherit=true) AND NOT (admin-[:MEMBER_OF]->()-[:DENIED]->()<-[:CHILD_OF*0..3]-company)\n" +
                        "RETURN DISTINCT admin, p";

                String notInheritedQuery = "MATCH (company:Company{name:{companyName}})\n" +
                        "MATCH p=company<-[permission:ALLOWED]-()<-[:MEMBER_OF]-admin\n" +
                        "WHERE (permission.inherit=false)\n" +
                        "RETURN DISTINCT admin, p";

                Map<String, Object> params = new HashMap<>();
                params.put( "companyName", companyName );

                return executionEngine.execute( params, inheritedQuery, notInheritedQuery );
    }

    public QueryUnionExecutionResult hasAccessToResource( String adminName, String resourceName )
    {
        String inheritedQuery =
                        "MATCH (admin:Administrator {name:{adminName}}),\n" +
                                "      (resource:Resource{name:{resourceName}})\n" +
                                "MATCH p=(admin)-[:MEMBER_OF]->()-[permission:ALLOWED]->()<-[:CHILD_OF*0." +
                                ".3]-(company)-[:WORKS_FOR|HAS_ACCOUNT*1..2]-(resource)\n" +
                                "WHERE (permission.inherit=true) AND NOT (admin)-[:MEMBER_OF]->()-[:DENIED]->()<-[:CHILD_OF*0..3]-(company)\n" +
                                "RETURN COUNT(p) AS accessCount";

                String notInheritedQuery =
                        "MATCH (admin:Administrator {name:{adminName}}),\n" +
                                "      (resource:Resource{name:{resourceName}})\n" +
                                "MATCH p=admin-[:MEMBER_OF]->()-[permission:ALLOWED]->company-[:WORKS_FOR|HAS_ACCOUNT*1..2]-resource\n" +
                                "WHERE (permission.inherit=false)\n" +
                                "RETURN COUNT(p) AS accessCount";

                Map<String, Object> params = new HashMap<>();
                params.put( "adminName", adminName );
                params.put( "resourceName", resourceName );

                return executionEngine.execute( params, inheritedQuery, notInheritedQuery );
    }

    public QueryUnionExecutionResult hasAccessToIndexedResource( String adminName, String resourceName )
    {
        String inheritedQuery =
                        "MATCH (admin:Administrator {name:{adminName}}),\n" +
                                "      (company)<-[:CHILD_OF*0..3]-(:Company)-[:WORKS_FOR|HAS_ACCOUNT*1..2]-(resource:Resource {name:{resourceName}})\n" +
                                "MATCH p=(admin)-[:MEMBER_OF]->()-[permission:ALLOWED]->(company)\n" +
                                "WHERE (permission.inherit=true) AND NOT ((admin)-[:MEMBER_OF]->()-[:DENIED]->()<-[:CHILD_OF*0..3]-(company))\n" +
                                "RETURN COUNT(p) AS accessCount";

                String notInheritedQuery =
                        "MATCH (admin:Administrator {name:{adminName}}),\n" +
                                "      (company)<-[:CHILD_OF*0..3]-(:Company)-[:WORKS_FOR|HAS_ACCOUNT*1..2]-(resource:Resource {name:{resourceName}})\n" +
                                "MATCH p=(admin)-[:MEMBER_OF]->()-[permission:ALLOWED]->(company)\n" +
                                "WHERE (permission.inherit=false)\n" +
                                "RETURN COUNT(p) AS accessCount";

                Map<String, Object> params = new HashMap<>();
                params.put( "adminName", adminName );
                params.put( "resourceName", resourceName );

                return executionEngine.execute( params, inheritedQuery, notInheritedQuery );
    }
}
