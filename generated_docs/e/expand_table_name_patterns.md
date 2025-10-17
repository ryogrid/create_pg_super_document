# expand_table_name_patterns

## Location
[src/bin/pg_dump/pg_dump.c:1613-1708](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/bin/pg_dump/pg_dump.c#L1613-L1708)

## Overview
Finds the OIDs of all tables (including relations, sequences, views, materialized views, foreign tables, and partitioned tables) matching the given list of patterns and appends them to the provided OID list, with optional inclusion of child tables through inheritance.

## Definition

```c
static void
expand_table_name_patterns(Archive *fout,
						   SimpleStringList *patterns, SimpleOidList *oids,
						   bool strict_names, bool with_child_tables)
```
## Detailed Description
This function is the most complex of the pattern expansion functions, processing table name patterns that can include schema qualification and optionally following inheritance relationships to include child tables. It queries the PostgreSQL system catalog pg_class along with pg_namespace to resolve patterns to table OIDs.

The function supports multiple relation types:
- Regular tables (RELKIND_RELATION)
- Sequences (RELKIND_SEQUENCE) 
- Views (RELKIND_VIEW)
- Materialized views (RELKIND_MATVIEW)
- Foreign tables (RELKIND_FOREIGN_TABLE)
- Partitioned tables (RELKIND_PARTITIONED_TABLE)

Key features include:
1. Constructs complex SELECT queries against pg_class with optional schema joins
2. Uses processSQL    ePattern for pattern matching with schema qualification support
3. Temporarily resets search_path for security and predictable behavior
4. Optionally includes child tables via recursive CTE for inheritance hierarchies
5. Validates qualified names don't exceed schema.table format
6. Enforces cross-database reference restrictions

When with_child_tables is true, the function uses a recursive CTE (Common Table Expression) to traverse the inheritance tree and include all child tables of matching parents.

## Parameters / Member Variables
- `*fout`: Archive structure containing database connection and dump context
- `*patterns`: SimpleStringList containing table name patterns to match (may include wildcards and schema qualification)
- `*oids`: SimpleOidList to append matching table OIDs to
- `strict_names`: Boolean flag that causes failure if any pattern matches no tables
- `with_child_tables`: Boolean flag to include child tables via inheritance relationships
## Dependencies
- Functions called/Symbols referenced:
  - [createPQExpBuffer](../c/createPQExpBuffer.md), resetPQExpBuffer, destroyPQExpBuffer (query buffer management)
  - processSQL    ePattern (pattern matching and SQL generation with schema support)
  - [GetConnection](../G/GetConnection.md) (database connection retrieval)
  - [ExecuteSqlStatement](../E/ExecuteSqlStatement.md), ExecuteSqlQuery, ExecuteSqlQueryForSingleRow (query execution)
  - [prohibit_crossdb_refs](../p/prohibit_crossdb_refs.md) (cross-database reference validation)
  - [simple_oid_list_append](../s/simple_oid_list_append.md) (OID list management)
  - atooid (string to OID conversion)
  - [pg_fatal](../p/pg_fatal.md) (error reporting)
  - RELKIND_* constants (relation type identification)
  - ALWAYS_SECURE_SEARCH_PATH_SQL (secure search path restoration)
- Called from (representative examples):
  - [main](../m/main.md) (in pg_dump.c at lines 891, 894, 902, 905, 909, 912)
  - fmtQualifiedDumpable (in pg_dump.c at line 197)

## Notes and Other Information
- The function temporarily resets search_path to avoid unqualified name resolution issues and ensure predictable behavior
- Supports schema-qualified patterns in the format schema.table, with validation for proper qualification levels
- The recursive CTE feature allows dumping entire inheritance hierarchies when with_child_tables is enabled
- Cross-database references are explicitly prohibited and will cause fatal errors when detected
- Pattern matching works with standard SQL wildcards and supports both qualified and unqualified patterns
- The function is designed to be absolutely devoid of unqualified names in its queries for security reasons
- When strict_names is true, any pattern that produces no matches will cause a fatal error
- The function handles multiple relation types, making it suitable for dumping various database object types
- Duplicate OIDs in the result list are allowed and handled by the caller

## Simplified Source

```c
static void
expand_table_name_patterns(Archive *fout,
                          SimpleStringList *patterns, SimpleOidList *oids,
                          bool strict_names, bool with_child_tables)
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
        PQExpBufferData dbbuf;
        int dotcnt;

        // Build base query with optional recursive CTE for child tables
        if (with_child_tables)
        {
            appendPQExpBuffer(query, "WITH RECURSIVE partition_tree (relid) AS (\n");
        }

        // Main SELECT from pg_class with namespace join
        appendPQExpBuffer(query,
                         "SELECT c.oid"
                         "\nFROM pg_catalog.pg_class c"
                         "\n     LEFT JOIN pg_catalog.pg_namespace n"
                         "\n     ON n.oid OPERATOR(pg_catalog.=) c.relnamespace"
                         "\nWHERE c.relkind OPERATOR(pg_catalog.=) ANY"
                         "\n    (array['%c', '%c', '%c', '%c', '%c', '%c'])\n",
                         RELKIND_RELATION, RELKIND_SEQUENCE, RELKIND_VIEW,
                         RELKIND_MATVIEW, RELKIND_FOREIGN_TABLE,
                         RELKIND_PARTITIONED_TABLE);

        // Process pattern with schema qualification support
        initPQExpBuffer(&dbbuf);
        processSQLNamePattern(GetConnection(fout), query, cell->val, true,
                             false, "n.nspname", "c.relname", NULL,
                             "pg_catalog.pg_table_is_visible(c.oid)", &dbbuf, &dotcnt);

        // Validate qualified names (schema.table format max)
        if (dotcnt > 2)
            pg_fatal("improper relation name (too many dotted names): %s", cell->val);
        else if (dotcnt == 2)
            prohibit_crossdb_refs(GetConnection(fout), dbbuf.data, cell->val);

        termPQExpBuffer(&dbbuf);

        // Add recursive part for child tables via inheritance
        if (with_child_tables)
        {
            appendPQExpBuffer(query, "UNION"
                             "\nSELECT i.inhrelid"
                             "\nFROM partition_tree p"
                             "\n     JOIN pg_catalog.pg_inherits i"
                             "\n     ON p.relid OPERATOR(pg_catalog.=) i.inhparent"
                             "\n)"
                             "\nSELECT relid FROM partition_tree");
        }

        // Execute with secure search_path handling
        ExecuteSqlStatement(fout, "RESET search_path");
        res = ExecuteSqlQuery(fout, query->data, PGRES_TUPLES_OK);
        PQclear(ExecuteSqlQueryForSingleRow(fout, ALWAYS_SECURE_SEARCH_PATH_SQL));

        // Check for matches if strict mode
        if (strict_names && PQntuples(res) == 0)
            pg_fatal("no matching tables were found for pattern \"%s\"", cell->val);

        // Add all matching OIDs to the list
        for (int i = 0; i < PQntuples(res); i++)
        {
            simple_oid_list_append(oids, atooid(PQgetvalue(res, i, 0)));
        }

        PQclear(res);
        resetPQExpBuffer(query);
    }

    destroyPQExpBuffer(query);
}
```