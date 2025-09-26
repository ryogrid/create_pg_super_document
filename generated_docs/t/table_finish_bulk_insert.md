# table_finish_bulk_insert

## Location
[src/include/access/tableam.h:1596-1621](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/include/access/tableam.h#L1596-L1621)

## Overview
Performs cleanup and finalization operations after completing bulk insert operations that were initiated with a BulkInsertState.

## Definition
```c
static inline void
table_finish_bulk_insert(Relation rel, int options)
```

## Detailed Description
This function serves as a completion handler for bulk insert operations that were performed using table_tuple_insert() or table_multi_insert() with a BulkInsertState specified. It provides an opportunity for table access methods to perform any necessary cleanup, finalization, or optimization operations after a series of bulk insertions.

The function is designed as an optional callback mechanism - table access methods that don't require special finalization can simply not implement this function pointer. For access methods that do implement it, this function might perform operations such as:
- Flushing buffered data to disk
- Updating table statistics
- Optimizing newly inserted data structures
- Releasing bulk insert resources
- Performing final consistency checks

The function acts as a wrapper around the table access method's finish_bulk_insert implementation, maintaining interface consistency across different storage engines.

## Parameters / Member Variables
- `rel`: The relation where bulk insert operations were performed
- `options`: Bitmask of options controlling finalization behavior (same options used during the bulk insert operations)

## Dependencies
- Functions called/Symbols referenced:
  - rel->rd_tableam->finish_bulk_insert (optional table access method function pointer)
- Types referenced:
  - None directly referenced
- Called from (representative examples):
  - [CopyMultiInsertBufferCleanup](../C/CopyMultiInsertBufferCleanup.md) (in src/backend/commands/copyfrom.c:503)
  - [intorel_shutdown](../i/intorel_shutdown.md) (in src/backend/commands/createas.c:615)
  - [transientrel_shutdown](transientrel_shutdown.md) (in src/backend/commands/matview.c:526)
  - [ATRewriteTable](../A/ATRewriteTable.md) (in src/backend/commands/tablecmds.c:6354)

## Notes and Other Information
- This is an optional callback - not all table access methods need to implement finish_bulk_insert
- The function checks if the table access method and its finish_bulk_insert function pointer exist before calling
- Should be called after completing a series of bulk insertions that used BulkInsertState
- The options parameter should match the options used during the corresponding bulk insert operations
- Commonly used in operations like COPY, CREATE TABLE AS, materialized view refresh, and table rewrites
- Failure to call this function after bulk operations may leave the table access method in an inconsistent state
- The function performs no operation if the table access method doesn't provide a finish_bulk_insert implementation