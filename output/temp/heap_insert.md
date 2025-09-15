# heap_insert

## Overview
Inserts a new tuple into a heap table, handling MVCC versioning and triggering necessary maintenance operations. This function is a core component of PostgreSQL's storage layer and manages transaction visibility.

## Definition
```c
HeapTuple heap_insert(Relation relation, HeapTuple tup, CommandId cid, 
                      int options, BulkInsertState bistate)
```

## Detailed Description
heap_insert performs the fundamental operation of adding a new tuple to a PostgreSQL heap table. It manages transaction visibility information through MVCC by setting appropriate transaction IDs. The function handles various optimization strategies including bulk insert operations and coordinates with the buffer manager for proper page allocation.

## Parameters / Member Variables
- `relation`: The target relation (table) where the tuple will be inserted, must be a valid heap relation
- `tup`: The HeapTuple structure containing the data to be inserted, includes header and data
- `cid`: Command ID for MVCC visibility determination within the current transaction
- `options`: Bitmask of insert options controlling behavior like WAL logging
- `bistate`: State for bulk insert optimizations, can be NULL for single inserts

## Dependencies
- **Called functions/Referenced symbols**:
  - `RelationGetBufferForTuple` - Allocates buffer space for the new tuple
  - `PageAddItem` - Adds the tuple to the page
- **Called from (representative examples)**:
  - `table_tuple_insert` - Generic table insertion interface
  - `ExecInsert` - Executor node for INSERT statements

## Notes & Other Information
This function is performance-critical and includes optimizations for bulk operations. Must be called within a valid transaction context.