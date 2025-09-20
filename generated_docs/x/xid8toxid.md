# xid8toxid

## Location
[src/backend/utils/adt/xid.c:174-181](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/xid.c#L174-L181)

## Overview
Converts a FullTransactionId (XID8) to a TransactionId (XID), effectively extracting the 32-bit transaction ID portion from the 64-bit full transaction identifier.

## Definition

```c
Datum
xid8toxid(PG_FUNCTION_ARGS)
```
## Detailed Description
The  function is a PostgreSQL type conversion function that converts from the XID8 data type (64-bit FullTransactionId) to the XID data type (32-bit TransactionId). This function extracts the lower 32-bit transaction ID from the full 64-bit transaction identifier, essentially performing a narrowing conversion that may lose the epoch information.

The function follows PostgreSQL's function call convention using the PG_FUNCTION_ARGS interface and returns a Datum. It retrieves the FullTransactionId input parameter and uses the XidFromFullTransactionId utility function to extract the 32-bit transaction ID portion.

## Parameters / Member Variables
- Input parameter (via PG_FUNCTION_ARGS): A FullTransactionId (XID8) value to be converted

## Dependencies
- Functions called/Symbols referenced:
  -  - Extracts FullTransactionId from function arguments
  -  - Extracts 32-bit TransactionId from FullTransactionId
  -  - Returns TransactionId as PostgreSQL Datum
- Types referenced:
  -  - 64-bit transaction identifier type
- Called from: 
  - No direct callers found (likely invoked through PostgreSQL's type system)

## Notes and Other Information
- This conversion is potentially lossy as it discards the epoch information contained in the FullTransactionId
- The function is used internally by PostgreSQL's type system for implicit and explicit casts from XID8 to XID
- Located in src/backend/utils/adt/xid.c, which contains transaction ID utility functions