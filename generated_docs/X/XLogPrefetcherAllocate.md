# XLogPrefetcherAllocate

## Location
[src/backend/access/transam/xlogprefetcher.c:362-391](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/access/transam/xlogprefetcher.c#L362-L391)

## Overview
Creates and initializes a new XLogPrefetcher instance that manages WAL record prefetching during recovery operations.

## Definition
```c
XLogPrefetcher *XLogPrefetcherAllocate(XLogReaderState *reader)
```

## Detailed Description
XLogPrefetcherAllocate allocates and initializes a complete WAL prefetcher system. The function sets up the core data structures needed for tracking and filtering blocks during WAL replay, including a hash table for maintaining per-relation filters and a doubly-linked list queue for managing filter lifecycle.

The function initializes shared statistics counters to zero and sets up the prefetcher for lazy allocation of the streaming read infrastructure. The reconfigure_count is set to trigger immediate reconfiguration on first use, ensuring optimal performance parameters are established when prefetching begins.

## Parameters / Member Variables
- `reader`: XLogReaderState pointer that provides the WAL reading context and will be associated with this prefetcher

## Dependencies
- Functions called/Symbols referenced:
  - [palloc0](../p/palloc0.md) (memory allocation)
  - [hash_create](../h/hash_create.md) (hash table creation)
  - [dlist_init](../d/dlist_init.md) (doubly-linked list initialization)
- Called from (representative examples):
  - [InitWalRecovery](../I/InitWalRecovery.md) (during WAL recovery initialization)

## Notes and Other Information
- Uses a static HASHCTL structure to configure the filter hash table with RelFileLocator keys and XLogPrefetcherFilter entries
- The hash table is sized to 1024 initial buckets and uses HASH_ELEM | HASH_BLOBS flags for efficient binary key comparison
- Shared statistics (wal_distance, block_distance, io_depth) are reset to zero, providing a clean state for new recovery sessions
- The streaming_read component is allocated lazily on first actual use, optimizing memory usage when prefetching might not be needed
- The prefetcher maintains a filter_queue using PostgreSQL's doubly-linked list implementation for efficient filter management

## Simplified Source

```c
// Simplified version of XLogPrefetcherAllocate
XLogPrefetcher *XLogPrefetcherAllocate(XLogReaderState *reader) {
    // Set up hash table configuration for filter management
    static HASHCTL hash_table_ctl = {
        .keysize = sizeof(RelFileLocator),
        .entrysize = sizeof(XLogPrefetcherFilter)
    };

    // Allocate and zero-initialize the main prefetcher structure
    XLogPrefetcher *prefetcher = palloc0(sizeof(XLogPrefetcher));

    // Associate with the WAL reader and create filter hash table
    prefetcher->reader = reader;
    prefetcher->filter_table = hash_create("XLogPrefetcherFilterTable", 1024,
                                          &hash_table_ctl,
                                          HASH_ELEM | HASH_BLOBS);

    // Initialize the filter queue for managing filter lifecycle
    dlist_init(&prefetcher->filter_queue);

    // Reset shared statistics for clean state
    SharedStats->wal_distance = 0;
    SharedStats->block_distance = 0;
    SharedStats->io_depth = 0;

    // Set up for lazy allocation of streaming reader on first use
    prefetcher->reconfigure_count = XLogPrefetchReconfigureCount - 1;

    return prefetcher;
}
```

Key simplifications made:
- Consolidated variable declarations with initialization where possible
- Added descriptive comments explaining each major setup phase
- Removed complex formatting of function call parameters for readability
- Simplified the static hash table control structure presentation
- Grouped related initialization operations with explanatory comments
- Maintained all essential functionality while improving code clarity