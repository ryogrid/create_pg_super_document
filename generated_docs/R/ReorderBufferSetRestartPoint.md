# ReorderBufferSetRestartPoint

## Location
[src/backend/replication/logical/reorderbuffer.c:1083-1094](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/replication/logical/reorderbuffer.c#L1083-L1094)

## Overview
Sets the restart point for logical decoding in the reorder buffer, establishing where decoding should resume from after a restart or interruption.

## Definition
```c
void ReorderBufferSetRestartPoint(ReorderBuffer *rb, XLogRecPtr ptr)
```

## Detailed Description
ReorderBufferSetRestartPoint is a simple but critical function that updates the restart point for logical decoding within a reorder buffer. The restart point represents the Log Sequence Number (LSN) position from which logical decoding should resume if the process is restarted or interrupted.

This function directly assigns the provided LSN pointer to the reorder buffer's current_restart_decoding_lsn field. This restart point is essential for recovery scenarios where logical replication needs to resume processing from a known safe point, ensuring no changes are lost or duplicated during the restart process.

The restart point is typically set during snapshot serialization and restoration operations, providing a consistent recovery point for logical replication slots.

## Parameters / Member Variables
- `rb`: Pointer to a ReorderBuffer structure where the restart point will be set
- `ptr`: XLogRecPtr value representing the LSN position that should serve as the new restart point for logical decoding

## Dependencies
- Functions called/Symbols referenced:
  - (None - this is a simple assignment function)
- Data structures used:
  - ReorderBuffer
  - XLogRecPtr
- Called from (representative examples):
  - SnapBuildSerialize (at src/backend/replication/logical/snapbuild.c:1895)
  - SnapBuildRestore (at src/backend/replication/logical/snapbuild.c:2059)

## Notes and Other Information
- This is a straightforward setter function with no validation or side effects
- Critical for recovery and restart scenarios in logical replication
- The restart point ensures logical decoding can resume from a consistent state
- Typically used in conjunction with snapshot serialization and restoration processes
- The set restart point affects where future decoding operations will begin processing WAL records