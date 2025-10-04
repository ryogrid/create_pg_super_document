# RestorePendingSyncs

## Location
[src/backend/catalog/storage.c:635-656](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/catalog/storage.c#L635-L656)

## Overview
RestorePendingSyncs restores pending synchronization operations within a parallel worker by deserializing RelFileLocator entries from memory.

## Definition
```c
void RestorePendingSyncs(char *startAddress)
```

## Detailed Description
This function is the counterpart to SerializePendingSyncs, designed to run in parallel worker processes. It reads a null-terminated array of RelFileLocator entries from the provided memory address and adds each one to the pending sync hash using AddPendingSync. This allows parallel workers to inherit the sync responsibilities from the main process.

The function intentionally does not restore the is_truncated field from the original PendingRelSync entries, as noted in the comments. This is because only smgrDoPendingSyncs() reads this field at end of transaction, and the simplified restoration is sufficient for the parallel worker's needs.

## Parameters / Member Variables
- `startAddress`: Memory address containing serialized RelFileLocator entries to restore

## Dependencies
- Functions called/Symbols referenced:
  - [AddPendingSync](../A/AddPendingSync.md)
- Called from (representative examples):
  - [ParallelWorkerMain](../P/ParallelWorkerMain.md)

## Notes and Other Information
- The function assumes pendingSyncHash is NULL when called (asserted)
- The is_truncated field is intentionally not restored since it's only needed by smgrDoPendingSyncs() at transaction end
- This ensures RelationNeedsWAL() and RelFileLocatorSkippingWAL() work correctly in parallel workers
- The input is expected to be a null-terminated array where the terminating entry has relNumber == 0

## Simplified Source

```c
void RestorePendingSyncs(char *startAddress) {
    // Ensure we start with empty pending sync hash
    Assert(pendingSyncHash == NULL);

    // Iterate through the serialized RelFileLocator array
    RelFileLocator *rlocator = (RelFileLocator *) startAddress;
    while (rlocator->relNumber != 0) {
        // Add each RelFileLocator to pending sync tracking
        AddPendingSync(rlocator);
        rlocator++;
    }
}
```