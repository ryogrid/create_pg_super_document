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
- : Archive structure containing database connection and dump context
- : SimpleStringList containing table name patterns to match (may include wildcards and schema qualification)
- : SimpleOidList to append matching table OIDs to
- : Boolean flag that causes failure if any pattern matches no tables
- : Boolean flag to include child tables via inheritance relationships

## Dependencies
- Functions called/Symbols referenced:
  - createPQExpBuffer, resetPQExpBuffer, destroyPQExpBuffer (query buffer management)
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