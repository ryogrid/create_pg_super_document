# xid8_smaller

## Location
[src/backend/utils/adt/xid.c:303-321](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/xid.c#L303-L321)

## Overview
Returns the smaller of two full transaction IDs (xid8), implementing the smaller() function for PostgreSQL's xid8 data type.

## Definition
```c
Datum xid8_smaller(PG_FUNCTION_ARGS)
```

## Detailed Description
The xid8_smaller function implements a minimum operation for PostgreSQL's full transaction ID (xid8) data type. It compares two FullTransactionId values and returns the one that is considered "smaller" in the transaction ordering system. The function uses FullTransactionIdPrecedes to determine which transaction ID comes earlier in the sequence, properly handling the wraparound nature of transaction IDs. If the first transaction ID precedes the second, it returns the first; otherwise, it returns the second.

## Parameters / Member Variables
- `PG_FUNCTION_ARGS`: Standard PostgreSQL function argument structure containing:
  - Argument 0: First FullTransactionId (fxid1) to compare
  - Argument 1: Second FullTransactionId (fxid2) to compare

## Dependencies
- Functions called/Symbols referenced:
  - [FullTransactionId](../F/FullTransactionId.md) (type)
  - PG_GETARG_FULLTRANSACTIONID (macro for argument extraction)
  - FullTransactionIdPrecedes (ordering comparison function)
  - PG_RETURN_FULLTRANSACTIONID (macro for returning transaction ID)
- Called from (representative examples):
  - SQL smaller() function calls on xid8 columns
  - Aggregate operations requiring minimum transaction ID
  - Internal PostgreSQL utilities

## Notes and Other Information
- Provides a safe way to find the minimum transaction ID without wraparound issues
- Essential for operations that need to determine the earliest transaction
- Handles edge cases where transaction IDs may have wrapped around
- Returns a FullTransactionId Datum, not a boolean like comparison operators
- Complement to xid8_larger function
- Located in src/backend/utils/adt/xid.c:303-321

## Simplified Source

```c
Datum xid8_smaller(PG_FUNCTION_ARGS) {
    // Get the two transaction IDs to compare
    FullTransactionId fxid1 = PG_GETARG_FULLTRANSACTIONID(0);
    FullTransactionId fxid2 = PG_GETARG_FULLTRANSACTIONID(1);

    // Return whichever transaction ID is smaller (comes earlier)
    if (FullTransactionIdPrecedes(fxid1, fxid2))
        PG_RETURN_FULLTRANSACTIONID(fxid1);
    else
        PG_RETURN_FULLTRANSACTIONID(fxid2);
}
```