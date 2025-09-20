# prepare_btree_command

## Location
[src/bin/pg_amcheck/pg_amcheck.c:881-929](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/bin/pg_amcheck/pg_amcheck.c#L881-L929)

## Overview
This function constructs a SQL command for running amcheck verification on a btree index relation using PostgreSQL's bt_index_check() or bt_index_parent_check() functions.

## Definition

```c
static void
prepare_btree_command(PQExpBuffer sql, RelationInfo *rel, PGconn *conn)
```
## Detailed Description
The prepare_btree_command function creates a SQL query that invokes btree checking functions from the amcheck extension to validate the integrity of a btree index. The function:

1. **Selects the appropriate checking function** based on opts.parent_check flag:
   - Uses bt_index_parent_check() for comprehensive parent-child relationship verification when parent_check is enabled
   - Uses bt_index_check() for basic structural verification when parent_check is disabled

2. **Configures verification parameters** including heapallindexed option, rootdescend behavior, and conditional checkunique functionality

3. **Filters problematic indexes** by excluding:
   - Temporary indexes (relpersistence != 't')
   - Indexes that are not ready, valid, or live (using pg_index.indisready, indisvalid, indislive)

4. **Handles error reporting** through PostgreSQL's error mechanism rather than returning result columns, as btree checking functions report corruption by raising errors

## Parameters / Member Variables
- : PQExpBuffer into which the constructed SQL command will be written
- : RelationInfo structure containing information about the index to be checked, including relation OID and database info
- : PGconn connection handle used for string escaping purposes (though not actively used in current implementation)

## Dependencies
- Functions called/Symbols referenced:
  - [resetPQExpBuffer](../r/resetPQExpBuffer.md) (clears the SQL buffer)
  - [appendPQExpBuffer](../a/appendPQExpBuffer.md) (appends formatted text to buffer)
  - [RelationInfo](../R/RelationInfo.md) (structure type for relation metadata)
- Called from (representative examples):
  - [main](../m/main.md) (in pg_amcheck.c:790)

## Notes and Other Information
- The function is static, meaning it's only accessible within the pg_amcheck.c compilation unit
- Unlike heap checking commands, btree checking commands don't select any result columns since corruption information is communicated through PostgreSQL errors
- The function automatically skips concurrent reindex operations by filtering on index readiness and validity flags
- Uses global opts structure to access configuration options like parent_check, heapallindexed, rootdescend, and checkunique settings
- The bt_index_parent_check() function provides more thorough checking by verifying parent-child relationships in the btree structure
- Located in src/bin/pg_amcheck/pg_amcheck.c:881-929