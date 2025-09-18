# xid_age

## Location
src/backend/utils/adt/xid.c: 104 - 119

## Overview
The `xid_age` function computes the age of a transaction ID (XID) relative to the latest stable transaction ID in PostgreSQL.

## Definition
```c
Datum xid_age(PG_FUNCTION_ARGS)
```

## Detailed Description
This function calculates how many transactions have occurred since a given transaction ID by comparing it to the current stable latest transaction ID. The age is computed as a simple arithmetic difference (latest_xid - input_xid). For special cases like permanent XIDs (InvalidTransactionId, BootstrapTransactionId, or FrozenTransactionId), the function returns INT_MAX to indicate they are infinitely old. This function is useful for monitoring transaction age for vacuum and freeze operations.

## Parameters / Member Variables
- `PG_FUNCTION_ARGS`: PostgreSQL function argument structure containing:
  - First argument: `TransactionId xid` - The transaction ID whose age is to be computed

## Dependencies
- Functions called/Symbols referenced:
  - PG_GETARG_TRANSACTIONID (macro for extracting TransactionId arguments)
  - GetStableLatestTransactionId (function to get the latest stable transaction ID)
  - TransactionIdIsNormal (macro to check if XID is a normal transaction ID)
  - PG_RETURN_INT32 (macro for returning 32-bit integer values)
- Called from (representative examples):
  - No direct references found in the codebase

## Notes and Other Information
- Permanent XIDs (Invalid, Bootstrap, Frozen) are considered infinitely old and return INT_MAX
- The age calculation is a simple arithmetic subtraction: (current_stable_xid - input_xid)
- This function is commonly used in system administration and monitoring contexts
- The result is returned as a 32-bit signed integer
- Located in src/backend/utils/adt/xid.c:104-119