# consume_xids_until

## Location
src/test/modules/xid_wraparound/xid_wraparound.c: 53 - 70

## Overview
A PostgreSQL test function that consumes transaction IDs until reaching a specified target transaction ID, used for testing XID wraparound scenarios.

## Definition
```c
Datum consume_xids_until(PG_FUNCTION_ARGS)
```

## Detailed Description
This function is part of the xid_wraparound test module and serves as a SQL-callable function to advance the system's transaction ID counter until it reaches a specified target transaction ID. Unlike consume_xids which takes a count of XIDs to consume, this function takes a target FullTransactionId and consumes XIDs until that target is reached. It validates that the target transaction ID is a normal (valid) transaction ID before proceeding. The actual XID consumption logic is delegated to the internal consume_xids_common function.

## Parameters / Member Variables
- `targetxid`: The target FullTransactionId to consume XIDs until (FullTransactionId). Must be a normal/valid transaction ID.

## Dependencies
- Functions called/Symbols referenced:
  - PG_GETARG_FULLTRANSACTIONID (macro for extracting FullTransactionId argument)
  - FullTransactionIdIsNormal (validates if transaction ID is normal/valid)
  - U64FromFullTransactionId (converts FullTransactionId to uint64 for logging)
  - [consume_xids_common](consume_xids_common.md) (internal function that performs actual XID consumption)
  - PG_RETURN_FULLTRANSACTIONID (macro for returning FullTransactionId result)
- Called from:
  - [consume_xids](consume_xids.md) (references this function in PG_FUNCTION_INFO_V1 declaration)

## Notes and Other Information
- This is a test-only function located in src/test/modules/xid_wraparound/
- Validates that targetxid is a normal transaction ID, throwing an error for invalid XIDs
- Uses PostgreSQL's function calling conventions with PG_FUNCTION_ARGS
- Returns a FullTransactionId representing the last consumed transaction ID
- Part of testing infrastructure for XID wraparound scenarios in PostgreSQL
- Complementary to consume_xids function - this consumes until a target, while consume_xids consumes a specific count