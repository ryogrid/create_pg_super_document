# table_tuple_get_latest_tid

## Location
[src/backend/access/table/tableam.c:236-276](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/access/table/tableam.c#L236-L276)

## Overview
Retrieves the latest tuple identifier (TID) for a given TID, following any tuple update chains or redirections to find the current version.

## Definition
```c
void table_tuple_get_latest_tid(TableScanDesc scan, ItemPointer tid)
```

## Detailed Description
This function updates a provided TID to point to the latest version of a tuple, following any update chains or HOT (Heap Only Tuple) redirections. It performs validation to ensure the input TID is valid and includes safety checks to prevent calls during logical decoding operations. The function delegates to the table access method's tuple_get_latest_tid implementation, allowing different storage engines to handle tuple versioning appropriately. The TID parameter is modified in-place to contain the latest tuple location.

## Parameters / Member Variables
- `scan`: TableScanDesc containing the table scan context and relation information
- `tid`: ItemPointer that is both input (original TID) and output (updated to latest TID)

## Dependencies
- Functions called/Symbols referenced:
  - TransactionIdIsValid
  - CheckXidAlive
  - bsysscan
  - elog
  - [ItemPointerGetBlockNumberNoCheck](../I/ItemPointerGetBlockNumberNoCheck.md)
  - [ItemPointerGetOffsetNumberNoCheck](../I/ItemPointerGetOffsetNumberNoCheck.md)
  - RelationGetRelationName
  - ereport
  - [TableAmRoutine](../T/TableAmRoutine.md)
- Called from (representative examples):
  - [TidNext](../T/TidNext.md) (TID scan execution)
  - [currtid_internal](../c/currtid_internal.md) (current TID SQL function)
  - [table_tuple_tid_valid](table_tuple_tid_valid.md) (inline helper in tableam.h)

## Dependencies
- Functions called/Symbols referenced:
  - tuple_tid_valid (via tableam)
  - tuple_get_latest_tid (via tableam)
  - TransactionIdIsValid
  - ereport/elog for error handling

## Notes and Other Information
- Includes runtime validation for logical decoding scenarios where direct calls are unexpected
- Performs input validation on user-supplied TIDs to prevent invalid access attempts
- The TID parameter is modified in-place to contain the result
- Part of PostgreSQL's table access method abstraction for storage engine independence
- Primarily used by TID scan operations and SQL functions that need to follow tuple update chains
- Error handling includes detailed error messages with specific TID coordinates for debugging