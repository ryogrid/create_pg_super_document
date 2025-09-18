# pg_xact_status

## Location
src/backend/utils/adt/xid8funcs.c: 640 - 683

## Overview
Returns the current status of a transaction ID as a text string, indicating whether the transaction is in progress, committed, aborted, or too old to determine.

## Definition


## Detailed Description
The  function is a PostgreSQL built-in function that reports the current status of a given transaction ID. It takes a full transaction ID (xid8) and returns one of several possible status strings: "in progress", "committed", "aborted", or NULL for transactions that are too old to determine status.

The function implements careful concurrency control by acquiring the XactTruncationLock in shared mode to prevent concurrent truncation of CLOG (commit log) entries during the status check. This prevents I/O errors that could occur when trying to read truncated CLOG pages.

The function follows PostgreSQL's transaction visibility protocol by first checking if the transaction is still in the process array (in progress) before consulting the CLOG. This prevents a race condition where a transaction might appear committed in CLOG but still be in the process of cleaning up, which could lead to incorrect visibility determinations.

For transactions that are too old (wrapped around, truncated, or otherwise no longer available in the system), the function returns NULL rather than potentially incorrect status information.

## Parameters / Member Variables
- : Standard PostgreSQL function argument structure containing:
  - Argument 0:  - The transaction ID whose status should be checked

## Dependencies
- Functions called/Symbols referenced:
  - : Extracts FullTransactionId from function arguments
  - : Acquires XactTruncationLock in shared mode for safe CLOG access
  - : Checks if the transaction ID is recent enough to have reliable status information
  - : Checks if the transaction is currently active in the process array
  - : Checks CLOG to determine if the transaction committed
  - : Releases the previously acquired lock
  - : Converts C string to PostgreSQL text datum
  - : Returns text result to PostgreSQL function call framework
  - : Returns NULL for transactions too old to determine status
- Called from (representative examples):
  - No direct callers found (likely called via SQL function calls)

## Notes and Other Information
- This function provides SQL access to transaction status information for debugging and monitoring purposes
- Located in 
- Uses proper locking to prevent race conditions with CLOG truncation operations
- Follows the same visibility checking protocol used by the storage layer to avoid race conditions
- Returns NULL for wrapped, truncated, or otherwise too old transaction IDs rather than potentially incorrect status
- The function can report on subtransaction status independently of parent transaction status
- This is a SQL-callable function useful for transaction monitoring and debugging visibility issues
- Part of the xid8 function family that works with 64-bit transaction IDs