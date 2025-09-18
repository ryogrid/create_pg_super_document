# consume_xids

## Location
src/test/modules/xid_wraparound/xid_wraparound.c: 32 - 52

## Overview
A PostgreSQL test function that consumes a specified number of transaction IDs (XIDs) from the transaction ID counter for testing XID wraparound behavior.

## Definition


## Detailed Description
This function is part of the xid_wraparound test module and serves as a SQL-callable function to advance the system's transaction ID counter by consuming a specified number of XIDs. It takes a single int64 argument specifying how many XIDs to consume and returns the final transaction ID after consumption. When nxids is 0, it simply returns the current next transaction ID without consuming any XIDs. The actual XID consumption logic is delegated to the internal consume_xids_common function.

## Parameters / Member Variables
- : Number of transaction IDs to consume (int64). Must be >= 0. When 0, no XIDs are consumed and the current next XID is returned.

## Dependencies
- Functions called/Symbols referenced:
  - PG_GETARG_INT64 (macro for extracting int64 argument)
  - ReadNextFullTransactionId (reads current next transaction ID)
  - consume_xids_common (internal function that performs actual XID consumption)
  - InvalidFullTransactionId (constant representing invalid transaction ID)
  - PG_RETURN_FULLTRANSACTIONID (macro for returning FullTransactionId result)
- Called from:
  - SQL queries (as a user-defined function in test scenarios)

## Notes and Other Information
- This is a test-only function located in src/test/modules/xid_wraparound/
- Validates that nxids is non-negative, throwing an error for negative values
- Uses PostgreSQL's function calling conventions with PG_FUNCTION_ARGS
- Returns a FullTransactionId representing the last consumed transaction ID
- Part of testing infrastructure for XID wraparound scenarios in PostgreSQL