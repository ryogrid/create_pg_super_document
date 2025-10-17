# expand_foreign_server_name_patterns

## Location
[src/bin/pg_dump/pg_dump.c:1561-1612](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/bin/pg_dump/pg_dump.c#L1561-L1612)

## Overview
Finds the OIDs of all foreign servers matching the given list of patterns and appends them to the provided OID list for use in pg_dump filtering operations.

## Definition

```c
static void
expand_foreign_server_name_patterns(Archive *fout,
									SimpleStringList *patterns,
									SimpleOidList *oids)
```
## Detailed Description
This function processes a list of foreign server name patterns (which may include wildcards) and resolves them to actual foreign server OIDs by querying the PostgreSQL system catalog pg_foreign_server. It is part of the pattern expansion family of functions used in pg_dump for filtering database objects.

The function performs the following operations for each pattern:
1. Constructs a SELECT query against pg_catalog.pg_foreign_server
2. Processes the pattern using processSQL    ePattern to handle wildcards and special characters
3. Validates that the pattern is not a qualified name (foreign servers don't support qualification)
4. Executes the query and collects matching foreign server OIDs
5. Always enforces strict matching (fails if no matches found)

Unlike some other pattern expansion functions, this function does not have a strict_names parameter - it always requires that patterns match at least one foreign server, failing with a fatal error if no matches are found.

## Parameters / Member Variables
- `*fout`: Archive structure containing database connection and dump context
- `*patterns`: SimpleStringList containing foreign server name patterns to match (may include wildcards)
- `*oids`: SimpleOidList to append matching foreign server OIDs to
## Dependencies
- Functions called/Symbols referenced:
  - [createPQExpBuffer](../c/createPQExpBuffer.md), resetPQExpBuffer, destroyPQExpBuffer (query buffer management)
  - processSQL    ePattern (pattern matching and SQL generation)
  - [GetConnection](../G/GetConnection.md) (database connection retrieval)
  - [ExecuteSqlQuery](../E/ExecuteSqlQuery.md) (query execution)
  - [simple_oid_list_append](../s/simple_oid_list_append.md) (OID list management)
  - atooid (string to OID conversion)
  - [pg_fatal](../p/pg_fatal.md) (error reporting)
- Called from (representative examples):
  - [main](../m/main.md) (in pg_dump.c at line 916)
  - fmtQualifiedDumpable (in pg_dump.c at line 194)

## Notes and Other Information
- Foreign server names cannot be qualified, so any pattern containing dots (dotcnt > 0) results in a fatal error
- Unlike expand_schema_name_patterns and expand_extension_name_patterns, this function always enforces strict matching and does not have a strict_names parameter
- The function allows duplicate OIDs in the result list, as duplicates are handled by the caller
- Pattern matching supports standard SQL wildcards through the processSQL    ePattern function
- Foreign servers are database-wide objects and are not contained within schemas, which is why qualification is not allowed
- The function always fails with a fatal error if any pattern produces no matches, making it stricter than some other pattern expansion functions
- The function builds separate queries for each pattern rather than trying to combine them into a single query

## Simplified Source

```c
static void
expand_foreign_server_name_patterns(Archive *fout,
                                    SimpleStringList *patterns,
                                    SimpleOidList *oids)
{
    PQExpBuffer query;
    PGresult   *res;
    SimpleStringListCell *cell;

    if (patterns->head == NULL)
        return;  /* nothing to do */

    query = createPQExpBuffer();

    // Process each pattern in the list
    for (cell = patterns->head; cell; cell = cell->next)
    {
        int dotcnt;

        // Build SELECT query for pg_foreign_server
        appendPQExpBufferStr(query, "SELECT oid FROM pg_catalog.pg_foreign_server s\n");

        // Process pattern with SQL wildcards
        processSQLNamePattern(GetConnection(fout), query, cell->val, false,
                             false, NULL, "s.srvname", NULL, NULL, NULL, &dotcnt);

        // Foreign servers cannot be qualified - reject dotted names
        if (dotcnt > 0)
            pg_fatal("improper qualified name (too many dotted names): %s", cell->val);

        // Execute query and collect results
        res = ExecuteSqlQuery(fout, query->data, PGRES_TUPLES_OK);

        // Always enforce strict matching - no matches is an error
        if (PQntuples(res) == 0)
            pg_fatal("no matching foreign servers were found for pattern \"%s\"", cell->val);

        // Add all matching OIDs to the list
        for (int i = 0; i < PQntuples(res); i++)
            simple_oid_list_append(oids, atooid(PQgetvalue(res, i, 0)));

        PQclear(res);
        resetPQExpBuffer(query);
    }

    destroyPQExpBuffer(query);
}
```