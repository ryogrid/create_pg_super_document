# xid8_larger

## Location
src/backend/utils/adt/xid.c: 291 - 302

## Overview
Returns the larger of two full transaction IDs (xid8), implementing the larger() function for PostgreSQL's xid8 data type.

## Definition
```c
Datum xid8_larger(PG_FUNCTION_ARGS)
```

## Detailed Description
The xid8_larger function implements a maximum operation for PostgreSQL's full transaction ID (xid8) data type. It compares two FullTransactionId values and returns the one that is considered "larger" in the transaction ordering system. The function uses FullTransactionIdFollows to determine which transaction ID comes later in the sequence, properly handling the wraparound nature of transaction IDs. If the first transaction ID follows the second, it returns the first; otherwise, it returns the second.

## Parameters / Member Variables
- `PG_FUNCTION_ARGS`: Standard PostgreSQL function argument structure containing:
  - Argument 0: First FullTransactionId (fxid1) to compare
  - Argument 1: Second FullTransactionId (fxid2) to compare

## Dependencies
- Functions called/Symbols referenced:
  - FullTransactionId (type)
  - PG_GETARG_FULLTRANSACTIONID (macro for argument extraction)
  - FullTransactionIdFollows (ordering comparison function)
  - PG_RETURN_FULLTRANSACTIONID (macro for returning transaction ID)
- Called from (representative examples):
  - SQL larger() function calls on xid8 columns
  - Aggregate operations requiring maximum transaction ID
  - Internal PostgreSQL utilities

## Notes and Other Information
- Provides a safe way to find the maximum transaction ID without wraparound issues
- Essential for operations that need to determine the most recent transaction
- Handles edge cases where transaction IDs may have wrapped around
- Returns a FullTransactionId Datum, not a boolean like comparison operators
- Located in src/backend/utils/adt/xid.c:291-302