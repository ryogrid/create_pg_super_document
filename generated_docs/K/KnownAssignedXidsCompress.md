# KnownAssignedXidsCompress

## Location
[src/backend/storage/ipc/procarray.c:4664-4677](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/storage/ipc/procarray.c#L4664-L4677)

## Overview
KnownAssignedXidsCompress compresses the KnownAssignedXids array by removing gaps and shifting valid transaction IDs to the beginning of the array, optimizing memory usage and search performance during Hot Standby operations.

## Definition

```c
static void
KnownAssignedXidsCompress(KAXCompressReason reason, bool haveLock)
```
## Detailed Description
This function implements the compression algorithm for the KnownAssignedXids array, which tracks active transaction IDs during Hot Standby recovery. The array can develop gaps over time as transactions end and their entries are marked invalid rather than immediately removed. Compression eliminates these gaps by copying all valid entries to the beginning of the array, improving both memory efficiency and search performance.

The function employs sophisticated heuristics to determine when compression should occur, balancing the O(S) cost of compression against the benefits of a more compact array. Different compression reasons trigger different decision logic: KAX_NO_SPACE forces immediate compression, while other reasons use frequency-based and time-based heuristics to avoid excessive compression overhead.

The compression process operates by iterating through the valid range of the array (tail to head), copying only valid entries to a compressed representation starting at index 0, then updating the array's head and tail pointers to reflect the new compact layout.

## Parameters / Member Variables
- : KAXCompressReason value indicating why compression is being considered (affects heuristic decisions)
- : Boolean indicating whether caller already holds ProcArrayLock in exclusive mode

## Dependencies
- Functions called/Symbols referenced:
  - [KAXCompressReason](KAXCompressReason.md) (enum type for compression reasons)
  - [ProcArrayStruct](../P/ProcArrayStruct.md) (main process array structure)
  - procArray (global process array instance)
  - KnownAssignedXids (global transaction ID array)
  - KnownAssignedXidsValid (global validity array)
  - LWLockAcquire/LWLockRelease (locking primitives)
  - TimestampTzPlusMilliseconds (timestamp arithmetic)
  - [GetCurrentTimestamp](../G/GetCurrentTimestamp.md) (current time function)
- Called from:
  - KnownAssignedTransactionIdsIdleMaintenance (idle maintenance)
  - KnownAssignedXidsAdd (when adding new XIDs)
  - KnownAssignedXidsRemoveTree (when removing XID trees)
  - KnownAssignedXidsRemovePreceding (when removing preceding XIDs)

## Notes and Other Information
- KAX_COMPRESS_FREQUENCY constant (128) controls how often compression occurs for transaction end events
- KAX_COMPRESS_IDLE_INTERVAL constant (1000ms) prevents too-frequent compression during idle periods
- Compression is forced only when reason is KAX_NO_SPACE, otherwise heuristics determine necessity
- Heuristic for KAX_TRANSACTION_END: compress only if array is less than 50% full and frequency threshold met
- Function is static (internal to procarray.c) and only called by startup process
- Requires ProcArrayLock in exclusive mode during the actual compression operation
- Updates lastCompressTs timestamp for future heuristic decisions
- The algorithm maintains the sorted property of the KnownAssignedXids array
- Compression complexity is O(S) where S is the number of elements between tail and head pointers