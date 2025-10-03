# SyncPreCheckpoint

## Location
[src/backend/storage/sync/sync.c:177-201](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/storage/sync/sync.c#L177-L201)

## Overview
Performs pre-checkpoint synchronization work by absorbing pending unlink requests and incrementing the checkpoint cycle counter to distinguish between requests that arrived before and during the checkpoint.

## Definition
```c
void SyncPreCheckpoint(void)
```

## Detailed Description
SyncPreCheckpoint is a critical function in PostgreSQL's checkpoint process that ensures proper handling of file unlink requests across checkpoint boundaries. It serves two main purposes: first, it absorbs all pending synchronization requests that were forwarded before the checkpoint began, ensuring they will be processed in the current checkpoint cycle. Second, it increments the checkpoint_cycle_ctr to create a clear demarcation point - any unlink requests that arrive after this point will be assigned to the next cycle and processed in a future checkpoint.

This timing is crucial for operations like DROP TABLESPACE, which depend on the guarantee that recently forwarded unlink requests will be processed in the next checkpoint. The function must be called before the checkpoint REDO point is determined to ensure files aren't deleted prematurely.

## Parameters / Member Variables

## Dependencies
- Functions called/Symbols referenced:
  - [AbsorbSyncRequests](../A/AbsorbSyncRequests.md) (absorbs pending sync requests from other processes)
  - checkpoint_cycle_ctr (global cycle counter variable)
- Called from (representative examples):
  - [CreateCheckPoint](../C/CreateCheckPoint.md) (main checkpoint creation function in xlog.c:6907)

## Notes and Other Information
- Must be called before the checkpoint REDO point is determined to prevent premature file deletion
- Cannot be called within a critical section due to memory allocations in AbsorbSyncRequests()
- The function cannot make assumptions about checkpoint completion, as the checkpoint might still fail after this point
- Essential for ensuring that DROP TABLESPACE and similar operations work correctly by guaranteeing their unlink requests are processed in the expected checkpoint cycle
- The cycle counter mechanism provides a clean separation between "old" and "new" unlink requests relative to the checkpoint boundary

## Simplified Source

```c
// Simplified version of SyncPreCheckpoint
void SyncPreCheckpoint(void) {
    // Step 1: Process all pending unlink requests that arrived before checkpoint
    // This ensures DROP TABLESPACE and similar operations work correctly
    AbsorbSyncRequests();

    // Step 2: Increment cycle counter to mark the checkpoint boundary
    // Any new unlink requests after this point go to the next checkpoint cycle
    checkpoint_cycle_ctr++;
}
```

Key simplifications made:
- Removed detailed comments for clarity while preserving essential explanations
- Focused on the two core operations: absorbing requests and incrementing counter
- Emphasized the checkpoint boundary concept with clear step-by-step comments
- Maintained the essential logic flow and purpose of the function