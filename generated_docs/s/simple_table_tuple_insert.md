# simple_table_tuple_insert

## Location
src/backend/access/table/tableam.c: 277 - 290

## Overview
A simplified wrapper function for inserting tuples that automatically provides default command ID and standard insertion options.

## Definition
```c
void simple_table_tuple_insert(Relation rel, TupleTableSlot *slot)
```

## Detailed Description
This function serves as a convenience wrapper around table_tuple_insert, providing a simplified interface for common tuple insertion scenarios. It automatically supplies the current command ID using GetCurrentCommandId(true) and uses default values for insertion options (no special flags, no bistate). This makes it suitable for straightforward insertion operations where advanced performance optimizations or specific transaction behavior is not required. The function delegates all actual insertion work to the underlying table_tuple_insert function.

## Parameters / Member Variables
- `rel`: The Relation object representing the target table for insertion
- `slot`: TupleTableSlot containing the tuple data to be inserted

## Dependencies
- Functions called/Symbols referenced:
  - table_tuple_insert
  - GetCurrentCommandId
- Called from (representative examples):
  - ExecSimpleRelationInsert (logical replication)
  - table_scan_sample_next_tuple (sampling operations)

## Notes and Other Information
- Designed for cases where default insertion behavior is sufficient
- Automatically handles command ID tracking for transaction visibility
- Does not provide access to advanced insertion options like bistate for bulk operations
- Part of PostgreSQL's simplified table access method interface
- Commonly used in replication and utility operations where insertion complexity is not needed
- The 'true' parameter to GetCurrentCommandId indicates this is a write operation that should increment the command counter