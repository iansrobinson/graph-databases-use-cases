package org.neo4j.graphdatabases.queries;

import java.util.HashMap;
import java.util.Map;

import org.neo4j.cypher.javacompat.ExecutionResult;
import org.neo4j.graphdatabases.queries.helpers.ExecutionEngineWrapper;

public class AccessControlNoAttributesQueries
{
    private final ExecutionEngineWrapper executionEngine;

    public AccessControlNoAttributesQueries( ExecutionEngineWrapper executionEngineWrapper )
    {
        this.executionEngine = executionEngineWrapper;
    }

    public ExecutionResult findAccessibleResources( String administratorName )
    {
        String query = "START admin=node:administrator(name={administratorName})\n" +
                "MATCH paths=admin-[:MEMBER_OF]->()-[:ALLOWED_INHERIT]->()<-[:CHILD_OF*0..3]-company\n" +
                "      <-[:WORKS_FOR]-employee-[:HAS_ACCOUNT]->account\n" +
                "WHERE NOT (admin-[:MEMBER_OF]->()-[:DENIED]->()<-[:CHILD_OF*0..3]-company)\n" +
                "RETURN employee.name AS employee, account.name AS account\n" +
                "UNION\n" +
                "START admin=node:administrator(name={administratorName})\n" +
                "MATCH paths=admin-[:MEMBER_OF]->()-[:ALLOWED_DO_NOT_INHERIT]->()\n" +
                "      <-[:WORKS_FOR]-employee-[:HAS_ACCOUNT]->account\n" +
                "RETURN employee.name AS employee, account.name AS account";

        Map<String, Object> params = new HashMap<String, Object>();
        params.put( "administratorName", administratorName );

        return executionEngine.execute( query, params );
    }

    public ExecutionResult findAccessibleCompanies( String administratorName )
    {
        String query = "START admin=node:administrator(name={administratorName})\n" +
                "MATCH admin-[:MEMBER_OF]->()-[:ALLOWED_INHERIT]->()<-[:CHILD_OF*0..3]-company\n" +
                "WHERE NOT (admin-[:MEMBER_OF]->()-[:DENIED]->()<-[:CHILD_OF*0..3]-company)\n" +
                "RETURN company.name AS company\n" +
                "UNION\n" +
                "START admin=node:administrator(name={administratorName})\n" +
                "MATCH admin-[:MEMBER_OF]->()-[:ALLOWED_DO_NOT_INHERIT]->(company)\n" +
                "RETURN company.name AS company";

        Map<String, Object> params = new HashMap<String, Object>();
        params.put( "administratorName", administratorName );

        return executionEngine.execute( query, params );
    }

    public ExecutionResult findAccessibleAccountsForCompany( String administratorName, String companyName )
    {
        String query =
                "START admin=node:administrator(name={administratorName}),\n" +
                        "      company=node:company(name={companyName})\n" +
                        "MATCH admin-[:MEMBER_OF]->group-[:ALLOWED_INHERIT]->company\n" +
                        "      <-[:CHILD_OF*0..3]-subcompany<-[:WORKS_FOR]-employee-[:HAS_ACCOUNT]->account\n" +
                        "WHERE NOT (admin-[:MEMBER_OF]->()-[:DENIED]->()<-[:CHILD_OF*0..3]-subcompany)\n" +
                        "RETURN account.name AS account\n" +
                        "UNION\n" +
                        "START admin=node:administrator(name={administratorName}),\n" +
                        "      company=node:company(name={companyName})\n" +
                        "MATCH admin-[:MEMBER_OF]->group-[:ALLOWED_DO_NOT_INHERIT]->company\n" +
                        "      <-[:WORKS_FOR]-employee-[:HAS_ACCOUNT]->account\n" +
                        "RETURN account.name AS account";

        Map<String, Object> params = new HashMap<String, Object>();
        params.put( "administratorName", administratorName );
        params.put( "companyName", companyName );

        return executionEngine.execute( query, params );
    }

    public ExecutionResult findAdminForResource( String resourceName )
    {
        String query = "START resource=node:resource(name={resourceName})\n" +
                       "MATCH p=resource-[:WORKS_FOR|HAS_ACCOUNT*1..2]-company\n" +
                       "        -[:CHILD_OF*0..3]->()<-[:ALLOWED_INHERIT]-()<-[:MEMBER_OF]-admin\n" +
                       "WHERE NOT (admin-[:MEMBER_OF]->()-[:DENIED]->()<-[:CHILD_OF*0..3]-company)\n" +
                       "RETURN admin.name AS admin\n" +
                       "UNION\n" +
                       "START resource=node:resource(name={resourceName})\n" +
                       "MATCH p=resource-[:WORKS_FOR|HAS_ACCOUNT*1..2]-company\n" +
                       "        <-[:ALLOWED_DO_NOT_INHERIT]-()<-[:MEMBER_OF]-admin\n" +
                       "RETURN admin.name AS admin";

        Map<String, Object> params = new HashMap<String, Object>();
        params.put( "resourceName", resourceName );

        return executionEngine.execute( query, params );
    }

    public ExecutionResult findAdminForCompany( String companyName )
    {
        String query = "START company=node:company(name={companyName})\n" +
                "MATCH company-[:CHILD_OF*0..3]->()<-[:ALLOWED_INHERIT]-()<-[:MEMBER_OF]-admin\n" +
                "WHERE NOT (admin-[:MEMBER_OF]->()-[:DENIED]->()<-[:CHILD_OF*0..3]-company)\n" +
                "RETURN admin.name AS admin\n" +
                "UNION\n" +
                "START company=node:company(name={companyName})\n" +
                "MATCH company<-[:ALLOWED_DO_NOT_INHERIT]-()<-[:MEMBER_OF]-admin\n" +
                "RETURN admin.name AS admin";

        Map<String, Object> params = new HashMap<String, Object>();
        params.put( "companyName", companyName );

        return executionEngine.execute( query, params );
    }

    public ExecutionResult hasAccessToResource( String adminName, String resourceName )
    {
        String query =
                "START admin=node:administrator(name={adminName}),\n" +
                        "      resource=node:resource(name={resourceName})\n" +
                        "MATCH p=admin-[:MEMBER_OF]->()-[:ALLOWED_INHERIT]->()<-[:CHILD_OF*0." +
                        ".3]-company-[:WORKS_FOR|HAS_ACCOUNT*1..2]-resource\n" +
                        "WHERE NOT (admin-[:MEMBER_OF]->()-[:DENIED]->()<-[:CHILD_OF*0..3]-company)\n" +
                        "RETURN COUNT(p) AS accessCount\n" +
                        "UNION\n" +
                        "START admin=node:administrator(name={adminName}),\n" +
                        "      resource=node:resource(name={resourceName})\n" +
                        "MATCH p=admin-[:MEMBER_OF]->()-[:ALLOWED_DO_NOT_INHERIT]->company-[:WORKS_FOR|HAS_ACCOUNT*1..2]-resource\n" +
                        "RETURN COUNT(p) AS accessCount";

        Map<String, Object> params = new HashMap<String, Object>();
        params.put( "adminName", adminName );
        params.put( "resourceName", resourceName );

        return executionEngine.execute( query, params );
    }


    public ExecutionResult hasAccessToIndexedResource( String adminName, String resourceName )
    {
        String query =
                "START admin=node:administrator(name={adminName}),\n" +
                        "      company=node:company(resourceName={resourceName})\n" +
                        "MATCH p=admin-[:MEMBER_OF]->()-[:ALLOWED_INHERIT]->()<-[:CHILD_OF*0..3]-company\n" +
                        "WHERE NOT (admin-[:MEMBER_OF]->()-[:DENIED]->()<-[:CHILD_OF*0..3]-company)\n" +
                        "RETURN COUNT(p) AS accessCount\n" +
                        "UNION\n" +
                        "START admin=node:administrator(name={adminName}),\n" +
                        "      company=node:company(resourceName={resourceName})\n" +
                        "MATCH p=admin-[:MEMBER_OF]->()-[:ALLOWED_DO_NOT_INHERIT]->company\n" +
                        "RETURN COUNT(p) AS accessCount";

        Map<String, Object> params = new HashMap<String, Object>();
        params.put( "adminName", adminName );
        params.put( "resourceName", resourceName );

        return executionEngine.execute( query, params );
    }
}
