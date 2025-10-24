# getRules

## Location
[src/bin/pg_dump/pg_dump.c:8124-8224](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/bin/pg_dump/pg_dump.c#L8124-L8224)

## Overview
Retrieves comprehensive information about all PostgreSQL rules in the system from the pg_rewrite catalog, preparing them for database dump operations.

## Definition

```c
RuleInfo *
getRules(Archive *fout, int *numRules)
```
## Detailed Description
This function queries the pg_rewrite system catalog to collect information about all rules defined in the database. Rules in PostgreSQL are used to implement views, materialized views, and custom query rewriting logic. The function creates RuleInfo structures for each rule, establishing proper dependency relationships between rules and their associated tables.

The function handles special cases for view and materialized view rules, particularly the ON SELECT rules that define view behavior. For these rules, it establishes dependencies to ensure proper dump ordering - view-defining rules are processed before their tables to handle dependencies correctly, while other rules are processed after their tables.

Key features include:
- Comprehensive rule information extraction from pg_rewrite catalog
- Proper dependency management between rules and tables
- Special handling for view and materialized view SELECT rules
- Memory allocation and structure initialization for all discovered rules
- Integration with the dump system's dependency tracking

## Parameters / Member Variables
- `*fout`: Archive pointer containing database connection and dump context information
- `*numRules`: Output parameter that receives the total number of rules found
## Dependencies
- Functions called/Symbols referenced:
  - [RuleInfo](../R/RuleInfo.md) (struct type)
  - [createPQExpBuffer](../c/createPQExpBuffer.md), appendPQExpBufferStr (query building functions)
  - [ExecuteSqlQuery](../E/ExecuteSqlQuery.md) (query execution)
  - [PQntuples](../P/PQntuples.md), PQfnumber, PQgetvalue (libpq result processing functions)
  - [pg_malloc](../p/pg_malloc.md) (memory allocation)
  - atooid (string to OID conversion)
  - [AssignDumpId](../A/AssignDumpId.md) (dump ID assignment)
  - [findTableByOid](../f/findTableByOid.md) (table lookup function)
  - [addObjectDependency](../a/addObjectDependency.md) (dependency management)
  - [destroyPQExpBuffer](../d/destroyPQExpBuffer.md) (cleanup)
  - DO_RULE (object type enum)
  - RELKIND_VIEW, RELKIND_MATVIEW (relation kind enums)
  - PGRES_TUPLES_OK (libpq result status)

- Called from (representative examples):
  - [getSchemaData](getSchemaData.md) (primary caller during schema data collection phase)

## Notes and Other Information
- Returns a dynamically allocated array of RuleInfo structures
- The caller is responsible for managing the returned memory
- Includes sophisticated dependency logic for view rules: ON SELECT INSTEAD rules for views/materialized views are made dependencies of their tables, while other rules depend on their tables
- Handles rule enablement status (ev_enabled) for conditional rule execution
- Performs sanity checking to ensure referenced tables exist
- Rules for views may be merged into CREATE VIEW statements rather than dumped separately (separate = false)
- The function processes rules in OID order to ensure consistent dump output
- Fatal errors occur if referenced tables cannot be found, indicating catalog corruption

## Simplified Source

```c
RuleInfo *getRules(Archive *fout, int *numRules)
{
    PGresult   *res;
    int         ntups;
    PQExpBuffer query = createPQExpBuffer();
    RuleInfo   *ruleinfo;

    // Query all rules from pg_rewrite catalog
    appendPQExpBufferStr(query,
        "SELECT tableoid, oid, rulename, "
        "ev_class AS ruletable, ev_type, is_instead, ev_enabled "
        "FROM pg_rewrite ORDER BY oid");

    res = ExecuteSqlQuery(fout, query->data, PGRES_TUPLES_OK);
    ntups = PQntuples(res);
    *numRules = ntups;

    ruleinfo = (RuleInfo *) pg_malloc(ntups * sizeof(RuleInfo));

    // Extract column indices
    int i_tableoid = PQfnumber(res, "tableoid");
    int i_oid = PQfnumber(res, "oid");
    int i_rulename = PQfnumber(res, "rulename");
    int i_ruletable = PQfnumber(res, "ruletable");
    int i_ev_type = PQfnumber(res, "ev_type");
    int i_is_instead = PQfnumber(res, "is_instead");
    int i_ev_enabled = PQfnumber(res, "ev_enabled");

    // Process each rule
    for (int i = 0; i < ntups; i++)
    {
        Oid ruletableoid;

        // Initialize rule object
        ruleinfo[i].dobj.objType = DO_RULE;
        ruleinfo[i].dobj.catId.tableoid = atooid(PQgetvalue(res, i, i_tableoid));
        ruleinfo[i].dobj.catId.oid = atooid(PQgetvalue(res, i, i_oid));
        AssignDumpId(&ruleinfo[i].dobj);
        ruleinfo[i].dobj.name = pg_strdup(PQgetvalue(res, i, i_rulename));

        // Find and link to the rule's table
        ruletableoid = atooid(PQgetvalue(res, i, i_ruletable));
        ruleinfo[i].ruletable = findTableByOid(ruletableoid);

        if (ruleinfo[i].ruletable == NULL)
            pg_fatal("parent table with OID %u not found", ruletableoid);

        // Set namespace and dump flags from table
        ruleinfo[i].dobj.namespace = ruleinfo[i].ruletable->dobj.namespace;
        ruleinfo[i].dobj.dump = ruleinfo[i].ruletable->dobj.dump;

        // Extract rule properties
        ruleinfo[i].ev_type = *(PQgetvalue(res, i, i_ev_type));
        ruleinfo[i].is_instead = *(PQgetvalue(res, i, i_is_instead)) == 't';
        ruleinfo[i].ev_enabled = *(PQgetvalue(res, i, i_ev_enabled));

        // Handle dependency ordering for view rules
        if (ruleinfo[i].ruletable)
        {
            // ON SELECT INSTEAD rules for views/materialized views are dumped before table
            if ((ruleinfo[i].ruletable->relkind == RELKIND_VIEW ||
                 ruleinfo[i].ruletable->relkind == RELKIND_MATVIEW) &&
                ruleinfo[i].ev_type == '1' && ruleinfo[i].is_instead)
            {
                addObjectDependency(&ruleinfo[i].ruletable->dobj, ruleinfo[i].dobj.dumpId);
                ruleinfo[i].separate = false;  // Merge into CREATE VIEW
            }
            else
            {
                addObjectDependency(&ruleinfo[i].dobj, ruleinfo[i].ruletable->dobj.dumpId);
                ruleinfo[i].separate = true;
            }
        }
        else
        {
            ruleinfo[i].separate = true;
        }
    }

    PQclear(res);
    destroyPQExpBuffer(query);
    return ruleinfo;
}
```