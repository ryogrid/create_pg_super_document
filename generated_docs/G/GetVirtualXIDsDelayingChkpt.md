# GetVirtualXIDsDelayingChkpt

## Location
[src/backend/storage/ipc/procarray.c:3042-3087](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/storage/ipc/procarray.c#L3042-L3087)

## Overview
GetVirtualXIDsDelayingChkpt returns an array of virtual transaction IDs for transactions that are currently in critical sections and delaying checkpoint completion.

## Definition

```c
VirtualTransactionId *
GetVirtualXIDsDelayingChkpt(int *nvxids, int type)
```
## Detailed Description
This function identifies and collects virtual transaction IDs (VXIDs) of transactions that are currently in commit critical sections, preventing checkpoint completion. These transactions are identified by having specific delayChkptFlags bits set in their PGPROC structure.

The function serves as a diagnostic and coordination mechanism for checkpoint operations. During checkpoint processing, PostgreSQL needs to ensure that certain critical operations complete before the checkpoint can proceed. Transactions in critical sections (such as during commit processing) set delay flags to signal that they should not be interrupted by checkpoint completion.

Key characteristics of the function:
- Scans all active processes in the process array
- Filters processes based on the specified delay flag type
- Collects virtual transaction IDs from matching processes
- Returns a dynamically allocated array that must be freed by the caller
- The result may be somewhat indeterminate due to lockless flag updates, but this is acceptable for its intended use

The function accepts a type parameter that specifies which delayChkptFlags bits to check, allowing different types of critical sections to be monitored independently.

## Parameters / Member Variables
- : Output parameter, pointer to integer that receives the count of returned VXIDs
- : Bitmask specifying which delayChkptFlags bits to check for (must be non-zero)

Returns:
- : Dynamically allocated array of virtual transaction IDs that should be freed by caller

## Dependencies
- Functions called/Symbols referenced:
  - [palloc](../p/palloc.md)
  - LWLockAcquire/LWLockRelease
  - GET_VXID_FROM_PGPROC
  - VirtualTransactionIdIsValid
- Called from (representative examples):
  - [CreateCheckPoint](../C/CreateCheckPoint.md) (src/backend/access/transam/xlog.c:7143)
  - [CreateCheckPoint](../C/CreateCheckPoint.md) (src/backend/access/transam/xlog.c:7165)

## Notes and Other Information
- [Result](../R/Result.md) array is allocated with palloc and must be freed by caller
- The function checks delayChkptFlags without holding locks, making results somewhat indeterminate
- This indeterminacy is acceptable since the function's purpose is coordination rather than strict consistency
- Used primarily during checkpoint operations to identify blocking transactions
- Memory allocation uses arrayP->maxProcs as upper bound for array size
- Only returns virtual transaction IDs that are valid
- The type parameter must be non-zero (assertion enforced)
- Critical for checkpoint coordination and ensuring database consistency during checkpoint operations