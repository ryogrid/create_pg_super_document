# table_multi_insert

## Location
src/include/access/tableam.h: 1458 - 1491

## Overview
Inserts multiple tuples into a table in a single operation, providing performance benefits over individual tuple insertions by reducing WAL logging and page locking overhead.

## Definition


## Detailed Description
This function provides a high-level interface for inserting multiple tuples into a table simultaneously. It serves as a wrapper around the table access method's multi_insert operation, delegating the actual insertion work to the storage engine specific implementation (e.g., heap, columnar storage).

The function is designed to be more efficient than calling table_tuple_insert() in a loop because:
- The access method can batch WAL logging operations
- Page locking overhead can be reduced through batching
- Buffer management can be optimized for bulk operations

The function operates as an inline wrapper that calls the appropriate table access method's multi_insert function pointer, allowing different storage engines to provide their own optimized implementations.

## Parameters / Member Variables
- : The relation (table) into which tuples will be inserted
- : Array of TupleTableSlot pointers containing the tuples to be inserted
- : Number of tuples in the slots array
- : Command ID for transaction visibility and MVCC purposes
- : Bitmask of insertion options controlling behavior
- : Bulk insert state data for optimizing bulk operations

## Dependencies
- Functions called/Symbols referenced:
  - rel->rd_tableam->multi_insert (table access method function pointer)
- Types referenced:
  - BulkInsertStateData
  - CommandId
  - TM_Result
- Called from (representative examples):
  - CopyMultiInsertBufferFlush (in src/backend/commands/copyfrom.c:412)

## Notes and Other Information
- This function leaks memory into the current memory context. Callers should consider creating a temporary memory context if memory usage is a concern
- The function parameters are the same as table_tuple_insert() except for taking multiple tuples as input
- This is an inline function defined in the header file, so it has no separate implementation file
- Performance benefits depend on the underlying table access method's implementation of the multi_insert operation