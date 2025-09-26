# ProcNumberGetTransactionIds

## Location
src/backend/storage/ipc/procarray.c: 3159 - 3194

## Overview
Retrieves the transaction status information (XID, XMIN, subtransaction count, and overflow status) for a backend process identified by its process number.

## Definition
void ProcNumberGetTransactionIds(ProcNumber procNumber, TransactionId *xid, TransactionId *xmin, int *nsubxid, bool *overflowed)

## Detailed Description
ProcNumberGetTransactionIds provides a safe way to examine the transaction state of another PostgreSQL backend process. The function retrieves four critical pieces of transaction information: the current transaction ID (xid), the minimum transaction ID visible to this backend (xmin), the number of subtransactions, and whether the subtransaction array has overflowed.

The function uses proper locking (ProcArrayLock in shared mode) to ensure consistent reads of the transaction state, which is important because transaction IDs can change rapidly as backends start and commit transactions. Like other process introspection functions, the returned information may become stale immediately after the function returns due to concurrent activity.

All output parameters are initialized to safe default values (InvalidTransactionId for XIDs, 0 for counts, false for overflow) before attempting to read the actual values, ensuring predictable behavior even when the target process is not found or not active.

## Parameters / Member Variables
- `procNumber`: The process number of the target backend to examine
- `xid`: Output parameter for the backend's current transaction ID
- `xmin`: Output parameter for the backend's minimum visible transaction ID  
- `nsubxid`: Output parameter for the count of subtransactions
- `overflowed`: Output parameter indicating if the subtransaction array has overflowed

## Dependencies
- Functions called/Symbols referenced:
  - GetPGProcByNumber
  - LWLockAcquire
  - LWLockRelease
  - ProcGlobal (global variable access)
  - ProcArrayLock (global lock)
- Called from (representative examples):
  - pgstat_read_current_status

## Notes and Other Information
- Uses ProcArrayLock in shared mode to ensure consistent transaction state reads
- All output parameters are initialized to safe defaults before reading actual values
- Returns immediately with defaults if process number is invalid or backend is inactive
- The transaction information may become stale immediately after function return
- Critical for transaction visibility determination and statistics collection
- The function is declared in src/include/storage/procarray.h