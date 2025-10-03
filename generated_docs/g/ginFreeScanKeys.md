# ginFreeScanKeys

## Location
[src/backend/access/gin/ginscan.c:238-267](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/access/gin/ginscan.c#L238-L267)

## Overview
Releases and cleans up all current scan keys and associated resources in a GIN index scan, preparing for scan termination or restart.

## Definition

```c
void
ginFreeScanKeys(GinScanOpaque so)
```
## Detailed Description
The  function performs comprehensive cleanup of all scan-related resources in a GIN index scan. It iterates through all scan entries and releases buffers, frees item pointer lists, ends TID bitmap iterations, and frees TID bitmaps. After cleaning up individual entries, it resets the key memory context and clears all scan key references.

This function is essential for proper resource management in GIN scans, ensuring that buffers are released back to the buffer pool, memory is freed, and no resources are leaked when a scan is completed or restarted.

## Parameters
- : GIN scan opaque data structure containing all scan state and resources

## Dependencies
- Functions called/Symbols referenced:
  - [ReleaseBuffer](../R/ReleaseBuffer.md)
  - [pfree](../p/pfree.md)
  - [tbm_end_iterate](../t/tbm_end_iterate.md)
  - [tbm_free](../t/tbm_free.md)
  - [MemoryContextReset](../M/MemoryContextReset.md)
- Called from:
  - [gingetbitmap](gingetbitmap.md)
  - [ginrescan](ginrescan.md)
  - [ginendscan](ginendscan.md)

## Notes and Other Information
- Safely handles NULL keys by returning early if no keys exist
- Systematically releases all types of resources: buffers, lists, iterators, and bitmaps
- Resets the key memory context to free all key-related allocations at once
- Clears all scan state pointers to prevent dangling references
- Essential for proper resource cleanup in GIN index access method
- Called during scan termination, restart, and error recovery scenarios

## Simplified Source

```c
void ginFreeScanKeys(GinScanOpaque so) {
    uint32 i;

    // Early return if no keys to free
    if (so->keys == NULL)
        return;

    // Clean up each scan entry's resources
    for (i = 0; i < so->totalentries; i++) {
        GinScanEntry entry = so->entries[i];

        // Release buffer if held
        if (entry->buffer != InvalidBuffer)
            ReleaseBuffer(entry->buffer);

        // Free item pointer list
        if (entry->list)
            pfree(entry->list);

        // End TID bitmap iteration
        if (entry->matchIterator)
            tbm_end_iterate(entry->matchIterator);

        // Free TID bitmap
        if (entry->matchBitmap)
            tbm_free(entry->matchBitmap);
    }

    // Reset key memory context to free all key-related allocations
    MemoryContextReset(so->keyCtx);

    // Clear scan state pointers
    so->keys = NULL;
    so->nkeys = 0;
    so->entries = NULL;
    so->totalentries = 0;
}
```