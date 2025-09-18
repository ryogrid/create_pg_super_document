# DisplayMapping

## Location
[src/backend/replication/logical/reorderbuffer.c:5183-5210](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/replication/logical/reorderbuffer.c#L5183-L5210)

## Overview
A debugging utility function that logs detailed information about tuple command ID mappings stored in a hash table for logical replication analysis.

## Definition
```c
static void DisplayMapping(HTAB *tuplecid_data)
```

## Detailed Description
This static function serves as a diagnostic tool for PostgreSQL's logical replication system, specifically for debugging tuple command ID (cid) mappings. It iterates through all entries in a hash table containing ReorderBufferTupleCidEnt structures and logs comprehensive information about each mapping.

The function outputs debug information at DEBUG3 level, which includes:
- Database OID, tablespace OID, and relation number from the relation locator
- Block number and offset number from the tuple identifier (TID)
- Command ID minimum (cmin) and maximum (cmax) values

This information is crucial for understanding how PostgreSQL tracks which commands within a transaction have affected specific tuples, which is essential for maintaining consistency in logical replication scenarios.

## Parameters / Member Variables
- `tuplecid_data`: Hash table (HTAB) containing ReorderBufferTupleCidEnt entries that map tuple identifiers to command IDs

## Dependencies
- Functions called/Symbols referenced:
  - [hash_seq_init](../h/hash_seq_init.md)
  - [hash_seq_search](../h/hash_seq_search.md)
  - elog (with DEBUG3 level)
  - [ItemPointerGetBlockNumber](../I/ItemPointerGetBlockNumber.md)
  - [ItemPointerGetOffsetNumber](../I/ItemPointerGetOffsetNumber.md)
- Called from (representative examples):
  - Currently not referenced by any other functions (likely used conditionally for debugging)

## Notes and Other Information
- This is a static function, accessible only within reorderbuffer.c
- Uses DEBUG3 logging level, meaning output is only visible when PostgreSQL is configured with very verbose debugging
- The function appears to be primarily used for development and debugging purposes
- Provides detailed visibility into the internal state of tuple command ID mappings
- The logged information includes both spatial (database/tablespace/relation/block/offset) and temporal (cmin/cmax) aspects of tuple tracking
- Currently has no active callers in the codebase, suggesting it may be used conditionally or during development