# get_parallel_object_list

## Location
[src/bin/scripts/reindexdb.c:636-819](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/bin/scripts/reindexdb.c#L636-L819)

## Overview
Queries the database catalogs to build lists of objects (tables, schemas, indexes) suitable for parallel reindex operations.

## Definition

```c
static SimpleStringList *
get_parallel_object_list(PGconn *conn, ReindexType type,
						 SimpleStringList *user_list, bool echo)
```
## Detailed Description
This function prepares object lists for parallel reindex operations by querying PostgreSQL system catalogs. It transforms high-level reindex requests (database, schema, index) into concrete lists of tables that can be processed independently by parallel workers.

**Key behaviors by type**:

**REINDEX_DATABASE**: Queries pg_class and pg_namespace to find all user tables and materialized views in the database, excluding:
- System catalogs (pg_catalog schema)
- Temporary tables (relpersistence = 't')
- Results ordered by table size (relpages DESC) for optimal load balancing

**REINDEX_SCHEMA**: Builds a query to find all tables and materialized views within the specified schema list, using the same filtering criteria as database reindex.

**REINDEX_INDEX**: Complex processing that:
1. Queries pg_index to find parent tables for the specified indexes
2. Groups indexes by their parent table for conflict-free parallel processing
3. Orders by maximum index size per table, then by individual index sizes
4. Rebuilds the user_list to match the new table-based ordering
5. Returns both a table list and reorders the original index list

The function uses safe search_path queries and proper identifier encoding to handle special characters and schema-qualified names.

## Parameters / Member Variables
- : PostgreSQL database connection for catalog queries
- : Type of reindex operation (REINDEX_DATABASE, REINDEX_SCHEMA, REINDEX_INDEX)
- : Input list of user-specified objects (modified in-place for REINDEX_INDEX)
- : Whether to echo the catalog query (affects relation qualification)

## Dependencies
- Functions called/Symbols referenced:
  - [initPQExpBuffer](../i/initPQExpBuffer.md)
  - [termPQExpBuffer](../t/termPQExpBuffer.md)
  - [resetPQExpBuffer](../r/resetPQExpBuffer.md)
  - [appendPQExpBufferStr](../a/appendPQExpBufferStr.md)
  - [appendStringLiteralConn](../a/appendStringLiteralConn.md)
  - [appendQualifiedRelation](../a/appendQualifiedRelation.md)
  - [executeQuery](../e/executeQuery.md)
  - [PQntuples](../P/PQntuples.md)
  - [PQgetvalue](../P/PQgetvalue.md)
  - [PQclear](../P/PQclear.md)
  - [PQfinish](../P/PQfinish.md)
  - [PQclientEncoding](../P/PQclientEncoding.md)
  - [fmtQualifiedIdEnc](../f/fmtQualifiedIdEnc.md)
  - [simple_string_list_append](../s/simple_string_list_append.md)
  - [simple_string_list_destroy](../s/simple_string_list_destroy.md)
  - [pg_malloc0](../p/pg_malloc0.md)
  - CppAsString2 macros for relation constants
  - ReindexType enums (REINDEX_DATABASE, REINDEX_SCHEMA, REINDEX_INDEX, REINDEX_SYSTEM, REINDEX_TABLE)
- Called from (representative examples):
  - [reindex_one_database](../r/reindex_one_database.md) (reindexdb.c:339, 352, 368)

## Notes and Other Information
- Returns NULL if no matching objects are found, allowing the caller to handle empty result sets gracefully
- For REINDEX_INDEX, modifies the input user_list in-place to reorder indexes according to their parent table grouping
- Uses window functions (OVER PARTITION BY) for sophisticated ordering of index results by table size
- Generates fully qualified identifiers using proper encoding to handle special characters
- The function closes the database connection and returns NULL when no objects are found
- REINDEX_SYSTEM and REINDEX_TABLE cases are not implemented (Assert(false)) as they don't require object list expansion
- [Query](../Q/Query.md) results are ordered by object size (relpages) in descending order to enable better load balancing in parallel execution
- For index reindexing, ensures that indexes belonging to the same table are grouped together to avoid concurrent modification conflicts