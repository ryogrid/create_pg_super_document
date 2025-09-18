# simple_table_tuple_delete

## Location
[src/backend/access/table/tableam.c:291-335](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/access/table/tableam.c#L291-L335)

## Overview
A simplified wrapper function for deleting tuples that provides default parameters and comprehensive error handling for non-concurrent scenarios.

## Definition
```c
void simple_table_tuple_delete(Relation rel, ItemPointer tid, Snapshot snapshot)
```

## Detailed Description
This function serves as a convenience wrapper around table_tuple_delete, designed for scenarios where concurrent updates are not expected (such as when holding a relation lock). It automatically supplies the current command ID, uses default deletion options, and provides comprehensive error handling for all possible tuple modification results. The function expects successful deletion and treats any concurrent modification scenarios as errors, making it suitable for controlled environments where such conditions indicate programming errors or unexpected race conditions.

## Parameters / Member Variables
- `rel`: The Relation object representing the table containing the tuple to delete
- `tid`: ItemPointer specifying the exact location of the tuple to be deleted
- `snapshot`: Snapshot defining the visibility rules for the deletion operation

## Dependencies
- Functions called/Symbols referenced:
  - table_tuple_delete
  - [GetCurrentCommandId](../G/GetCurrentCommandId.md)
  - InvalidSnapshot
  - elog
  - TM_Result (return type enum)
  - TM_FailureData
  - TM_SelfModified, TM_Ok, TM_Updated, TM_Deleted (result constants)
- Called from (representative examples):
  - [ExecSimpleRelationDelete](../E/ExecSimpleRelationDelete.md) (logical replication)
  - table_scan_sample_next_tuple (sampling operations)

## Notes and Other Information
- Designed for use cases where concurrent tuple modifications should not occur
- Automatically handles command ID assignment and waits for commit completion
- Provides detailed error messages for all failure scenarios including self-modification, concurrent updates, and concurrent deletions
- Uses InvalidSnapshot as the crosscheck snapshot parameter, indicating no additional visibility checks
- The changingPart parameter is set to false, indicating this is not a partition change operation
- Part of PostgreSQL's simplified table access method interface for straightforward deletion operations
- Commonly used in replication and utility contexts where deletion failures indicate serious issues