# compile_relation_list_one_db

## Location
[src/bin/pg_amcheck/pg_amcheck.c:1883-2223](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/bin/pg_amcheck/pg_amcheck.c#L1883-L2223)

## Overview
Compiles a list of relations (tables and indexes) to check within the currently connected database based on user-supplied options, sorted by descending size, and appends them to the given list of relations.

## Definition

```c
static void
compile_relation_list_one_db(PGconn *conn, SimplePtrList *relations,
							 const DatabaseInfo *dat,
							 uint64 *pagecount)
```
## Detailed Description
This function is a core component of the PostgreSQL  utility that builds a comprehensive list of database relations to be checked by the amcheck extension. The function constructs a complex SQL query using Common Table Expressions (CTEs) to identify relations based on inclusion/exclusion patterns and various filtering options.

The function handles several key aspects:
- **Pattern Matching**: Processes inclusion and exclusion patterns for relation selection using regular expressions for database, namespace, and relation names
- **Relation Types**: Supports both heap tables (relam = HEAP_TABLE_AM_OID) and btree indexes (relam = BTREE_AM_OID)  
- **Dependent Objects**: Optionally includes associated toast tables and btree indexes based on command-line options (--no-dependent-toast, --no-dependent-indexes)
- **Sorting**: Orders results by descending page count to process larger relations first
- **Block Range Support**: Calculates actual blocks to check when --startblock/--endblock options are specified for heap tables

The constructed SQL query uses multiple CTEs:
- /: Processes inclusion patterns
- /: Processes exclusion patterns  
- : Main relation selection CTE
- : Toast table selection (if enabled)
- : Btree index selection (if enabled)
- : Toast table index selection (if enabled)

## Parameters / Member Variables
- : Active PostgreSQL connection to the database being processed
- : SimplePtrList to which discovered RelationInfo structures are appended
- : DatabaseInfo structure containing connection details and amcheck extension information for the relations
- : Pointer to uint64 counter that gets incremented by the total number of blocks to check across all added relations

## Dependencies
- Functions called/Symbols referenced:
  - [initPQExpBuffer](../i/initPQExpBuffer.md)
  - [append_rel_pattern_raw_cte](../a/append_rel_pattern_raw_cte.md)
  - [append_rel_pattern_filtered_cte](../a/append_rel_pattern_filtered_cte.md)
  - [executeQuery](../e/executeQuery.md)
  - [PQgetisnull](../P/PQgetisnull.md)
  - atooid
  - [pg_malloc0](../p/pg_malloc0.md)
  - [simple_ptr_list_append](../s/simple_ptr_list_append.md)
  - [termPQExpBuffer](../t/termPQExpBuffer.md)
  - [disconnectDatabase](../d/disconnectDatabase.md)
- Called from:
  - [main](../m/main.md) (src/bin/pg_amcheck/pg_amcheck.c:634)

## Notes and Other Information
- This function is specific to the  utility and operates within a single database context
- The function excludes temporary relations (relpersistence != 't') as they belong to other sessions
- [Complex](../C/Complex.md) logic handles the interaction between --allrel mode and specific inclusion patterns to avoid duplicate selection of dependent objects
- Error handling includes detailed query logging when SQL execution fails
- The function supports progress tracking by calculating expected block counts for heap table range checking
- Results are deduplicated using UNION operations to handle cases where relations match multiple patterns or appear in multiple CTEs