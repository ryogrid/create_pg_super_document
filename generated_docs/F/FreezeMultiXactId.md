# FreezeMultiXactId

## Location
[src/backend/access/heap/heapam.c:6659-7008](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/access/heap/heapam.c#L6659-L7008)

## Overview
A static function that determines how to handle MultiXactId values during tuple freezing, deciding whether to preserve, replace, or invalidate the MultiXactId based on vacuum cutoffs and the status of member transactions.

## Definition


## Detailed Description
FreezeMultiXactId is a critical component of PostgreSQL's tuple freezing mechanism that specifically handles MultiXactId values in tuple headers. During VACUUM operations, this function analyzes a MultiXactId and its member transactions to determine the appropriate action based on various age-based cutoffs.

The function implements sophisticated logic to:
1. Validate the MultiXactId and check for corruption
2. Handle very old MultiXactIds that predate cutoff limits  
3. Analyze individual member transactions within the MultiXactId
4. Decide whether to preserve, replace with a single XID, create a new MultiXactId, or invalidate the field entirely
5. Ensure proper freezing postconditions are maintained

The decision-making process considers multiple factors including transaction commit status, whether transactions are still running, and various vacuum cutoff thresholds. The function helps prevent transaction ID wraparound while maintaining data integrity.

## Parameters
- : The MultiXactId value to be processed during freezing
- : Tuple header infomask bits providing context about the MultiXactId
- : Structure containing various vacuum cutoff thresholds (FreezeLimit, OldestXmin, etc.)
- : Output parameter indicating what action the caller should take (FRM_NOOP, FRM_INVALIDATE_XMAX, etc.)
- : Input/output structure for managing page-level freezing state

## Dependencies
- Functions called/Symbols referenced:
  - MultiXactIdIsValid
  - [MultiXactIdPrecedes](../M/MultiXactIdPrecedes.md)
  - [MultiXactIdIsRunning](../M/MultiXactIdIsRunning.md)
  - [MultiXactIdGetUpdateXid](../M/MultiXactIdGetUpdateXid.md)
  - [GetMultiXactIdMembers](../G/GetMultiXactIdMembers.md)
  - [MultiXactIdCreateFromMembers](../M/MultiXactIdCreateFromMembers.md)
  - [TransactionIdPrecedes](../T/TransactionIdPrecedes.md)
  - [TransactionIdDidCommit](../T/TransactionIdDidCommit.md)
  - [TransactionIdIsCurrentTransactionId](../T/TransactionIdIsCurrentTransactionId.md)
  - TransactionIdIsInProgress
  - HEAP_XMAX_IS_MULTI
  - HEAP_LOCKED_UPGRADED
  - HEAP_XMAX_IS_LOCKED_ONLY
  - ISUPDATE_from_mxstatus
- Called from:
  - [heap_prepare_freeze_tuple](../h/heap_prepare_freeze_tuple.md)

## Notes and Other Information
- **Return Value Interpretation**: The returned TransactionId's meaning depends on the flags set:
  - With FRM_RETURN_IS_XID: Single XID to use as new xmax
  - With FRM_RETURN_IS_MULTI: New MultiXactId to use as new xmax
  - With FRM_INVALIDATE_XMAX: Return value should be ignored, xmax gets invalidated
  - With FRM_NOOP: Return value is the original multi, no changes needed
- **Page-Level Freezing**: The function coordinates with the caller to manage page-level freezing requirements
- **SLRU Optimization**: Designed to minimize MultiXact member SLRU buffer misses through proactive processing
- **Corruption Detection**: Includes extensive validation and error reporting for data corruption scenarios
- **Member Transaction Handling**: Distinguishes between locker and updater transactions, keeping only necessary ones
- **Vacuum Integration**: Works closely with vacuum cutoff management to ensure safe transaction ID advancement
- **Critical for MVCC**: Essential for maintaining PostgreSQL's MVCC (Multi-Version Concurrency Control) semantics during freezing