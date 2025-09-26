# table_index_fetch_end

## Location
src/include/access/tableam.h: 1212 - 1241

## Overview
Releases resources and deallocates an index fetch operation by cleaning up the IndexFetchTableData structure.

## Definition


## Detailed Description
This function is part of PostgreSQL's table access method (tableam) interface that provides a standardized way to terminate and clean up index fetch operations. When called, it invokes the table access method's specific `index_fetch_end` function pointer to deallocate any resources that were allocated during the lifetime of the index fetch operation.

This function represents the final cleanup phase of an index fetch operation, ensuring that all allocated memory, locks, or other resources are properly released. It's the counterpart to table_index_fetch_begin and should be called when the index fetch operation is completely finished.

The function serves as a thin wrapper around the table access method's implementation, allowing different storage engines to handle resource deallocation in their own specific way while maintaining a consistent interface.

## Parameters / Member Variables
- `scan`: Pointer to IndexFetchTableData structure containing the index fetch state and associated table relation information that needs to be deallocated

## Dependencies
- Functions called/Symbols referenced:
  - IndexFetchTableData (structure type)
  - rd_tableam->index_fetch_end (table access method function pointer)
- Called from (representative examples):
  - index_endscan (src/backend/access/index/indexam.c:386)
  - table_index_fetch_tuple_check (src/backend/access/table/tableam.c:223)
  - unique_key_recheck (src/backend/commands/constraint.c:120, 123)

## Notes and Other Information
- This is an inline function defined in the tableam.h header file
- Part of the table access method abstraction layer introduced to support pluggable storage engines
- The actual implementation is delegated to the specific table access method via a function pointer
- Should be paired with a corresponding table_index_fetch_begin call
- Critical for preventing resource leaks in index scanning operations