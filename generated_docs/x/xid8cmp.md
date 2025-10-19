# xid8cmp

## Location
[src/backend/utils/adt/xid.c:277-290](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/xid.c#L277-L290)

## Overview
Implements a three-way comparison function for full transaction IDs (xid8), returning -1, 0, or 1 based on whether the first transaction ID is less than, equal to, or greater than the second.

## Definition
```c
Datum xid8cmp(PG_FUNCTION_ARGS)
```

## Detailed Description
The xid8cmp function provides a comprehensive comparison mechanism for PostgreSQL's full transaction ID (xid8) data type. It extracts two FullTransactionId values from the function arguments and performs a three-way comparison using PostgreSQL's transaction ID ordering functions. The function returns 1 if the first transaction ID follows (is greater than) the second, 0 if they are equal, and -1 if the first precedes (is less than) the second. This ordering respects the wraparound nature of transaction IDs.

## Parameters / Member Variables
- `PG_FUNCTION_ARGS`: Standard PostgreSQL function argument structure containing:
  - Argument 0: First FullTransactionId (fxid1) to compare
  - Argument 1: Second FullTransactionId (fxid2) to compare against

## Dependencies
- Functions called/Symbols referenced:
  - [FullTransactionId](../F/FullTransactionId.md) (type)
  - PG_GETARG_FULLTRANSACTIONID (macro for argument extraction)
  - FullTransactionIdFollows (ordering comparison function)
  - FullTransactionIdEquals (equality comparison function)
- Called from (representative examples):
  - B-tree index operations for xid8 columns
  - ORDER BY clauses on xid8 columns
  - Internal PostgreSQL sorting operations

## Notes and Other Information
- This function is essential for xid8 indexing and sorting operations
- Provides the foundation for all xid8 comparison operators
- Handles transaction ID wraparound correctly through underlying utility functions
- Returns standard three-way comparison semantics (-1, 0, 1)
- Located in src/backend/utils/adt/xid.c:277-290

## Simplified Source

```c
Datum xid8cmp(PG_FUNCTION_ARGS) {
    // Extract the two 64-bit transaction IDs from function arguments
    FullTransactionId fxid1 = PG_GETARG_FULLTRANSACTIONID(0);
    FullTransactionId fxid2 = PG_GETARG_FULLTRANSACTIONID(1);

    // Perform three-way comparison
    if (FullTransactionIdFollows(fxid1, fxid2))
        PG_RETURN_INT32(1);    // fxid1 > fxid2
    else if (FullTransactionIdEquals(fxid1, fxid2))
        PG_RETURN_INT32(0);    // fxid1 == fxid2
    else
        PG_RETURN_INT32(-1);   // fxid1 < fxid2
}
```