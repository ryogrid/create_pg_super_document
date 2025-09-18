# ReplicationSlotShmemExit

## Location
src/backend/replication/slot.c: 233 - 251

## Overview
Cleanup function that releases active replication slots and cleans up temporary slots when a backend process exits.

## Definition
```c
static void ReplicationSlotShmemExit(int code, Datum arg)
```

## Detailed Description
This static function serves as an exit callback that ensures proper cleanup of replication slot resources when a backend process terminates. It performs two key cleanup operations: first, it releases any active replication slot currently held by the process (MyReplicationSlot), and second, it cleans up all temporary replication slots associated with the process.

The function is designed to handle both normal and abnormal process termination scenarios to prevent resource leaks. It follows PostgreSQL's standard exit callback pattern, accepting exit code and argument parameters as required by the before_shmem_exit callback mechanism.

## Parameters / Member Variables
- : Exit code from the terminating process (standard exit callback parameter)
- : Datum argument passed to the callback (standard exit callback parameter, unused here)

## Dependencies
- Functions called/Symbols referenced:
  - [ReplicationSlotRelease](ReplicationSlotRelease.md)
  - [ReplicationSlotCleanup](ReplicationSlotCleanup.md)
- Called from (representative examples):
  - [ReplicationSlotInitialize](ReplicationSlotInitialize.md) (via before_shmem_exit callback registration)

## Notes and Other Information
- This is a static function, only accessible within the slot.c compilation unit
- Automatically called during process termination through the before_shmem_exit callback mechanism
- Handles both persistent and temporary replication slot cleanup
- The ReplicationSlotCleanup call with false parameter indicates it should clean up temporary slots but not persistent ones owned by other processes
- Critical for preventing replication slot leaks that could exhaust system resources