# LogicalTapeSetCreate

## Location
[src/backend/utils/sort/logtape.c:556-608](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/sort/logtape.c#L556-L608)

## Overview
Creates and initializes a new logical tape set backed by a temporary file, supporting both single-process and parallel sorting operations with optional block preallocation for performance optimization.

## Definition
```c
LogicalTapeSet *LogicalTapeSetCreate(bool preallocate, SharedFileSet *fileset, int worker)
```

## Detailed Description
The `LogicalTapeSetCreate` function creates a new LogicalTapeSet structure that serves as a container for multiple logical tapes sharing a common underlying temporary file. The function supports three distinct usage patterns:

1. **Single-process sort**: Called with `fileset=NULL` and `worker=-1`, creates a conventional serial BufFile for tape storage.

2. **Parallel worker**: Called with a shared fileset and worker number, creates a worker-specific BufFile within the shared fileset using the worker number as the filename.

3. **Parallel leader**: Called with a shared fileset and `worker=-1`, creates a tape set that will import worker tapes rather than creating its own BufFile initially. The leader hijacks the first imported tape's BufFile and concatenates subsequent tapes to it.

The tape set is initially empty and requires `LogicalTapeCreate()` calls to add individual tapes. Block preallocation can be enabled to reduce fragmentation when multiple tapes are written simultaneously by allocating blocks in batches rather than individually.

## Parameters / Member Variables
- `preallocate`: If true, enables batch allocation of blocks for individual tapes to reduce fragmentation during concurrent writes
- `fileset`: Pointer to SharedFileSet for parallel operations, or NULL for single-process sorts
- `worker`: Worker identifier for parallel operations (-1 for leader/single-process, ≥0 for workers)

## Dependencies
- Functions called/Symbols referenced:
  - [palloc](../p/palloc.md) (memory allocation)
  - [pg_itoa](../p/pg_itoa.md) (integer to string conversion)
  - [BufFileCreateFileSet](../B/BufFileCreateFileSet.md) (creates shared fileset BufFile)
  - [BufFileCreateTemp](../B/BufFileCreateTemp.md) (creates temporary BufFile)
  - [LogicalTapeSet](LogicalTapeSet.md) (structure type)
  - SharedFileSet (shared file structure type)
- Called from (representative examples):
  - [hash_agg_enter_spill_mode](../h/hash_agg_enter_spill_mode.md) (hash aggregation spilling)
  - [inittapes](../i/inittapes.md) (tuplesort initialization)
  - [leader_takeover_tapes](../l/leader_takeover_tapes.md) (parallel sort leader)

## Notes and Other Information
- The tape set starts with reasonable initial values: 32 free block slots, no allocated/written blocks
- Memory tracking includes separate counters for allocated blocks, written blocks, and hole blocks
- The `forgetFreeSpace` flag controls whether free space tracking is disabled for performance
- Block preallocation is particularly beneficial for external merge sorts where multiple runs are written simultaneously
- In parallel scenarios, the leader does not create its own tapes but imports worker tapes using `LogicalTapeImport()`
- The underlying BufFile handles the actual I/O operations while LogicalTapeSet provides the logical abstraction
- Free block tracking uses a dynamically sized array that starts with 32 entries and grows as needed

## Simplified Source

```c
LogicalTapeSet *LogicalTapeSetCreate(bool preallocate, SharedFileSet *fileset, int worker) {
    LogicalTapeSet *lts;

    // Allocate and initialize the tape set structure
    lts = (LogicalTapeSet *) palloc(sizeof(LogicalTapeSet));
    lts->nBlocksAllocated = 0L;
    lts->nBlocksWritten = 0L;
    lts->nHoleBlocks = 0L;
    lts->forgetFreeSpace = false;
    lts->freeBlocksLen = 32;  // Initial free block array size
    lts->freeBlocks = (int64 *) palloc(lts->freeBlocksLen * sizeof(int64));
    lts->nFreeBlocks = 0;
    lts->enable_prealloc = preallocate;

    lts->fileset = fileset;
    lts->worker = worker;

    // Create underlying BufFile based on usage pattern
    if (fileset && worker == -1) {
        // Parallel leader - will import worker tapes later
        lts->pfile = NULL;
    } else if (fileset) {
        // Parallel worker - create worker-specific file
        char filename[MAXPGPATH];
        pg_itoa(worker, filename);
        lts->pfile = BufFileCreateFileSet(&fileset->fs, filename);
    } else {
        // Single-process sort - create temporary file
        lts->pfile = BufFileCreateTemp(false);
    }

    return lts;
}
```