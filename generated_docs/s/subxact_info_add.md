# subxact_info_add

## Location
[src/backend/replication/logical/worker.c:4119-4196](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/replication/logical/worker.c#L4119-L4196)

## Overview
Adds information about a subtransaction, specifically its offset in the main stream file, to the in-memory subtransaction tracking structure.

## Definition
```c
static void subxact_info_add(TransactionId xid)
```

## Detailed Description
This function manages the dynamic tracking of subtransactions during logical replication streaming. It maintains an array of SubXactInfo structures that record the file position where each subtransaction's first change appears in the stream file. The function implements several optimizations: it avoids duplicate entries, skips the toplevel transaction, and uses reverse linear search since recent subtransactions are more likely to be accessed. Memory management is handled dynamically with initial allocation of 128 entries and doubling when capacity is exceeded.

## Parameters / Member Variables
- `xid`: Transaction ID of the subtransaction to add

## Dependencies
- Functions called/Symbols referenced:
  - [palloc](../p/palloc.md)
  - [repalloc](../r/repalloc.md)
  - [BufFileTell](../B/BufFileTell.md)
  - [MemoryContextSwitchTo](../M/MemoryContextSwitchTo.md)
  - [SubXactInfo](../S/SubXactInfo.md)
  - LogicalStreamingContext
  - stream_xid (global variable)
  - stream_fd (global variable)
  - subxact_data (global structure)
- Called from (representative examples):
  - [handle_streamed_transaction](../h/handle_streamed_transaction.md)

## Notes and Other Information
- This is a static function with internal linkage within worker.c
- The function includes several performance optimizations:
  - Early return for toplevel transaction XIDs
  - Caching of last processed XID to avoid redundant processing
  - Reverse linear search from array tail for better cache locality
- Memory allocation occurs in LogicalStreamingContext for proper lifetime management
- Initial capacity is set to 128 SubXactInfo entries and doubles when full
- The function stores both file number and offset using BufFileTell for precise positioning
- Contains a TODO comment suggesting binary search optimization if XIDs arrive in sorted order
- Array scanning is intentionally done in reverse order since recent subtransactions are more likely to be accessed again

## Simplified Source

```c
static void subxact_info_add(TransactionId xid) {
    SubXactInfo *subxacts = subxact_data.subxacts;
    int64 i;

    Assert(TransactionIdIsValid(stream_xid));
    Assert(stream_fd != NULL);

    // Skip toplevel transaction
    if (stream_xid == xid)
        return;

    // Skip if we already processed this XID recently
    if (subxact_data.subxact_last == xid)
        return;

    // Remember this XID as last processed
    subxact_data.subxact_last = xid;

    // Check if transaction already exists (scan from tail for efficiency)
    for (i = subxact_data.nsubxacts; i > 0; i--) {
        if (subxacts[i - 1].xid == xid)
            return; // Already exists
    }

    // New subxact - ensure we have space in array
    if (subxact_data.nsubxacts == 0) {
        // Initial allocation
        MemoryContext oldctx;
        subxact_data.nsubxacts_max = 128;

        oldctx = MemoryContextSwitchTo(LogicalStreamingContext);
        subxacts = palloc(subxact_data.nsubxacts_max * sizeof(SubXactInfo));
        MemoryContextSwitchTo(oldctx);
    } else if (subxact_data.nsubxacts == subxact_data.nsubxacts_max) {
        // Double the array size
        subxact_data.nsubxacts_max *= 2;
        subxacts = repalloc(subxacts, subxact_data.nsubxacts_max * sizeof(SubXactInfo));
    }

    // Add new subtransaction info
    subxacts[subxact_data.nsubxacts].xid = xid;

    // Record current position in stream file
    BufFileTell(stream_fd,
                &subxacts[subxact_data.nsubxacts].fileno,
                &subxacts[subxact_data.nsubxacts].offset);

    subxact_data.nsubxacts++;
    subxact_data.subxacts = subxacts;
}
```