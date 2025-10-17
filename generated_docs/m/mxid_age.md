# mxid_age

## Location
[src/backend/utils/adt/xid.c:120-138](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/xid.c#L120-L138)

## Overview
The `mxid_age` function computes the age of a MultiXact ID (MXID) relative to the latest stable MultiXact ID in PostgreSQL.

## Definition
```c
Datum mxid_age(PG_FUNCTION_ARGS)
```

## Detailed Description
This function calculates how many MultiXact operations have occurred since a given MultiXact ID by comparing it to the current next MultiXact ID. MultiXacts are used in PostgreSQL to handle row-level locking when multiple transactions need to lock the same row with different lock modes. The age is computed as a simple arithmetic difference (next_mxid - input_mxid). For invalid MultiXact IDs, the function returns INT_MAX to indicate they are infinitely old. This function is useful for monitoring MultiXact wraparound and maintenance operations.

## Parameters / Member Variables
- `PG_FUNCTION_ARGS`: PostgreSQL function argument structure containing:
  - First argument: `TransactionId xid` - The MultiXact ID whose age is to be computed (note: despite the parameter name, this represents a MultiXact ID)

## Dependencies
- Functions called/Symbols referenced:
  - PG_GETARG_TRANSACTIONID (macro for extracting TransactionId arguments)
  - [ReadNextMultiXactId](../R/ReadNextMultiXactId.md) (function to get the next MultiXact ID)
  - MultiXactId (type definition for MultiXact IDs)
  - MultiXactIdIsValid (macro to check if MXID is valid)
  - PG_RETURN_INT32 (macro for returning 32-bit integer values)
- Called from (representative examples):
  - No direct references found in the codebase

## Notes and Other Information
- Invalid MultiXact IDs return INT_MAX to indicate infinite age
- The age calculation is a simple arithmetic subtraction: (next_mxid - input_mxid)
- MultiXacts are used for row-level locking scenarios with multiple lock holders
- This function helps monitor MultiXact wraparound, which is critical for database maintenance
- The result is returned as a 32-bit signed integer
- Located in src/backend/utils/adt/xid.c:120-138

## Simplified Source

```c
Datum mxid_age(PG_FUNCTION_ARGS) {
    TransactionId mxid = PG_GETARG_TRANSACTIONID(0);
    MultiXactId next_mxid = ReadNextMultiXactId();

    // Invalid MultiXact IDs are infinitely old
    if (!MultiXactIdIsValid(mxid))
        PG_RETURN_INT32(INT_MAX);

    // Calculate age as difference from next MultiXact ID
    PG_RETURN_INT32((int32) (next_mxid - mxid));
}
```