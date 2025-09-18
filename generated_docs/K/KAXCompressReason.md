# KAXCompressReason

## Location
src/backend/storage/ipc/procarray.c: 266 - 321

## Overview
KAXCompressReason is an enumeration that specifies the reason for compressing the KnownAssignedXids array during Hot Standby operations in PostgreSQL recovery processes.

## Definition


## Detailed Description
This enumeration is used internally by the procarray.c module to control when and why the KnownAssignedXids array should be compressed. The KnownAssignedXids array tracks transaction IDs that are known to be assigned during Hot Standby recovery, and over time it can develop gaps when transactions end. To maintain efficiency for operations like taking snapshots and searching for specific XIDs, the array needs periodic compression to remove these gaps and shift valid data to the beginning.

The compression is controlled by heuristics that determine the optimal time to perform this potentially expensive O(S) operation, where S is the number of elements in the array. Different reasons trigger different compression strategies - some force immediate compression while others only suggest it based on efficiency considerations.

## Parameters / Member Variables
- : Forces immediate compression because there's no space available at the array end for new entries
- : Suggests compression after old entries have been pruned from the array
- : Suggests compression after some transaction IDs have been committed or removed
- : Suggests compression when the startup process is about to become idle, providing a good opportunity for maintenance

## Dependencies
- Functions called/Symbols referenced:
  - [ProcArrayStruct](../P/ProcArrayStruct.md) (used in compression logic)
  - [PGPROC](../P/PGPROC.md) (process array structure)
  - [GlobalVisState](../G/GlobalVisState.md) (visibility state structures)
- Called from:
  - [KnownAssignedXidsCompress](KnownAssignedXidsCompress.md) (primary consumer of this enum)

## Notes and Other Information
- This enum is specific to Hot Standby recovery operations and is not used during normal database operations
- The compression heuristic typically triggers when the array size S >= 2N, where N is the number of valid XIDs
- KAX_NO_SPACE is the only reason that forces immediate compression regardless of heuristics
- The enum values are used to optimize the timing of expensive array maintenance operations during recovery
- Compression requires holding ProcArrayLock in exclusive mode, making the timing critical for performance