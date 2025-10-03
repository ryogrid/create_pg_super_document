# ImmediateCheckpointRequested

## Location
[src/backend/postmaster/checkpointer.c:687-713](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/postmaster/checkpointer.c#L687-L713)

## Overview
Checks whether an immediate checkpoint request is pending in the checkpointer's shared memory flags.

## Definition

```c
static bool
ImmediateCheckpointRequested(void)
```
## Detailed Description
ImmediateCheckpointRequested is a utility function that examines the checkpointer shared memory structure to determine if there is a pending request for an immediate checkpoint. The function specifically checks for the CHECKPOINT_IMMEDIATE flag in the shared memory flags.

The function is designed to check for pending immediate checkpoint requests, not the current checkpoint's immediate flag. This distinction is important because it allows the system to determine if there are high-priority checkpoint requests waiting to be processed even while a current checkpoint operation may be in progress.

The function performs a lockless read of the shared memory flag, which is safe because it only examines a single flag bit. This design choice provides better performance by avoiding lock contention while still providing accurate information about pending immediate checkpoints.

## Parameters / Member Variables

## Dependencies
- Functions called/Symbols referenced:
  - [CheckpointerShmemStruct](../C/CheckpointerShmemStruct.md) (shared memory structure)
  - CHECKPOINT_IMMEDIATE (flag constant)
- Called from (representative examples):
  - [CheckpointWriteDelay](../C/CheckpointWriteDelay.md) (checkpointer.c:728)

## Notes and Other Information
- Uses volatile qualifier for shared memory access to prevent compiler optimizations
- Performs lockless read for performance reasons since only checking a single bit
- Returns true only when CHECKPOINT_IMMEDIATE flag is set in pending requests
- Part of the checkpoint prioritization system that allows immediate checkpoints to bypass normal timing constraints
- Used primarily during checkpoint write delay calculations to determine if delays should be skipped

## Simplified Source

```c
static bool ImmediateCheckpointRequested(void)
{
    volatile CheckpointerShmemStruct *cps = CheckpointerShmem;

    // Check if immediate checkpoint flag is set (lockless read)
    if (cps->ckpt_flags & CHECKPOINT_IMMEDIATE)
        return true;
    return false;
}
```