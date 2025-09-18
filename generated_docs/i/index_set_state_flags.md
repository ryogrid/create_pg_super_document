# index_set_state_flags

## Location
[src/backend/catalog/index.c:3442-3521](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/catalog/index.c#L3442-L3521)

## Overview
Adjusts the state flags in pg_index during concurrent index operations to control index visibility and usability across different phases of creation and deletion.

## Definition
```c
void index_set_state_flags(Oid indexId, IndexStateFlagsAction action)
```

## Detailed Description
index_set_state_flags manages the lifecycle state flags of an index stored in the pg_index system catalog during concurrent index operations. These flags control when an index becomes visible to queries, when it starts being maintained by DML operations, and when it gets removed. The function handles four distinct state transitions:

1. **INDEX_CREATE_SET_READY**: Sets indisready=true during CREATE INDEX CONCURRENTLY, making the index available for DML maintenance while still not queryable
2. **INDEX_CREATE_SET_VALID**: Sets indisvalid=true during CREATE INDEX CONCURRENTLY, making the index fully available for queries
3. **INDEX_DROP_CLEAR_VALID**: Clears indisvalid=false during DROP INDEX CONCURRENTLY, removing query access while keeping DML maintenance
4. **INDEX_DROP_SET_DEAD**: Clears both indisready=false and indislive=false during DROP INDEX CONCURRENTLY, completely removing the index from use

The function ensures proper state transitions through assertions and clears related flags like indisclustered and indisreplident when appropriate. Updates are immediately visible to other sessions via cache invalidation.

## Parameters / Member Variables
- : Object identifier of the index whose state flags need to be modified
- : Enumerated value specifying which state transition to perform (INDEX_CREATE_SET_READY, INDEX_CREATE_SET_VALID, INDEX_DROP_CLEAR_VALID, or INDEX_DROP_SET_DEAD)

## Dependencies
- Functions called/Symbols referenced:
  - table_open
  - table_close
  - SearchSysCacheCopy1
  - [CatalogTupleUpdate](../C/CatalogTupleUpdate.md)
  - HeapTupleIsValid
  - GETSTRUCT
  - Form_pg_index
  - IndexStateFlagsAction
- Called from (representative examples):
  - [DefineIndex](../D/DefineIndex.md)
  - index_concurrently_build
  - [index_concurrently_set_dead](index_concurrently_set_dead.md)
  - [index_drop](index_drop.md)

## Notes and Other Information
- Critical component of PostgreSQL's concurrent index building and dropping mechanisms
- State transitions are protected by assertions to ensure proper sequencing
- Cache invalidation ensures immediate visibility of state changes to other sessions
- The function maintains consistency by clearing related flags (indisclustered, indisreplident) during drop operations
- Concurrent index operations minimize locking by carefully orchestrating these state transitions
- INDEX_DROP_CLEAR_VALID allows retrying failed DROP INDEX CONCURRENTLY operations
- The indisready flag controls whether DML operations maintain the index, while indisvalid controls query visibility