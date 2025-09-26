# LVRelState

## Location
[src/backend/access/heap/vacuumlazy.c:136-219](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/access/heap/vacuumlazy.c#L136-L219)

## Overview
LVRelState is the main state structure used by PostgreSQL's lazy vacuum implementation to maintain all necessary information during vacuum operations on heap relations.

## Definition
```c
typedef struct LVRelState
{
    /* Target heap relation and its indexes */
    Relation    rel;
    Relation   *indrels;
    int         nindexes;

    /* Buffer access strategy and parallel vacuum state */
    BufferAccessStrategy bstrategy;
    ParallelVacuumState *pvs;

    /* Aggressive VACUUM? (must set relfrozenxid >= FreezeLimit) */
    bool        aggressive;
    /* Use visibility map to skip? (disabled by DISABLE_PAGE_SKIPPING) */
    bool        skipwithvm;
    /* Consider index vacuuming bypass optimization? */
    bool        consider_bypass_optimization;

    /* Doing index vacuuming, index cleanup, rel truncation? */
    bool        do_index_vacuuming;
    bool        do_index_cleanup;
    bool        do_rel_truncate;

    /* VACUUM operation's cutoffs for freezing and pruning */
    struct VacuumCutoffs cutoffs;
    GlobalVisState *vistest;
    /* Tracks oldest extant XID/MXID for setting relfrozenxid/relminmxid */
    TransactionId NewRelfrozenXid;
    MultiXactId NewRelminMxid;
    bool        skippedallvis;

    /* Error reporting state */
    char       *dbname;
    char       *relnamespace;
    char       *relname;
    char       *indname;        /* Current index name */
    BlockNumber blkno;          /* used only for heap operations */
    OffsetNumber offnum;        /* used only for heap operations */
    VacErrPhase phase;
    bool        verbose;        /* VACUUM VERBOSE? */

    /* Dead items storage */
    TidStore   *dead_items;     /* TIDs whose index tuples we'll delete */
    VacDeadItemsInfo *dead_items_info;

    /* Page statistics */
    BlockNumber rel_pages;      /* total number of pages */
    BlockNumber scanned_pages;  /* # pages examined (not skipped via VM) */
    BlockNumber removed_pages;  /* # pages removed by relation truncation */
    BlockNumber frozen_pages;   /* # pages with newly frozen tuples */
    BlockNumber lpdead_item_pages; /* # pages with LP_DEAD items */
    BlockNumber missed_dead_pages; /* # pages with missed dead tuples */
    BlockNumber nonempty_pages; /* actually, last nonempty page + 1 */

    /* Statistics output by us, for table */
    double      new_rel_tuples; /* new estimated total # of tuples */
    double      new_live_tuples; /* new estimated total # of live tuples */
    /* Statistics output by index AMs */
    IndexBulkDeleteResult **indstats;

    /* Instrumentation counters */
    int         num_index_scans;
    /* Counters that follow are only for scanned_pages */
    int64       tuples_deleted; /* # deleted from table */
    int64       tuples_frozen;  /* # newly frozen */
    int64       lpdead_items;   /* # deleted from indexes */
    int64       live_tuples;    /* # live tuples remaining */
    int64       recently_dead_tuples; /* # dead, but not yet removable */
    int64       missed_dead_tuples; /* # removable, but not removed */

    /* State maintained by heap_vac_scan_next_block() */
    BlockNumber current_block;  /* last block returned */
    BlockNumber next_unskippable_block; /* next unskippable block */
    bool        next_unskippable_allvis; /* its visibility status */
    Buffer      next_unskippable_vmbuffer; /* buffer containing its VM bit */
} LVRelState;
```

## Detailed Description
LVRelState serves as the central coordination structure for PostgreSQL's lazy vacuum implementation. It maintains comprehensive state information throughout the vacuum process, including relation metadata, vacuum configuration options, progress tracking, statistics collection, and error reporting context. The structure supports both serial and parallel vacuum operations, tracking dead items for efficient index cleanup, and maintaining various counters for performance monitoring and reporting.

## Parameters / Member Variables
### Relation Information
- `rel`: The target heap relation being vacuumed
- `indrels`: Array of index relations associated with the heap
- `nindexes`: Number of indexes on the relation

### Vacuum Strategy and Parallelism
- `bstrategy`: Buffer access strategy for vacuum operations
- `pvs`: Parallel vacuum state (NULL for non-parallel vacuum)

### Vacuum Configuration
- `aggressive`: Whether this is an aggressive vacuum (must advance relfrozenxid)
- `skipwithvm`: Whether to use visibility map to skip pages
- `consider_bypass_optimization`: Whether to consider index vacuum bypass optimization
- `do_index_vacuuming`: Whether to perform index vacuuming
- `do_index_cleanup`: Whether to perform index cleanup
- `do_rel_truncate`: Whether to truncate the relation

### Transaction Management
- `cutoffs`: Vacuum cutoffs for freezing and pruning operations
- `vistest`: Global visibility state for tuple visibility testing
- `NewRelfrozenXid`: New value for relation's relfrozenxid
- `NewRelminMxid`: New value for relation's relminmxid
- `skippedallvis`: Whether all-visible pages were skipped

### Error Reporting Context
- `dbname`: Database name for error messages
- `relnamespace`: Schema name for error messages
- `relname`: Relation name for error messages
- `indname`: Current index name being processed
- `blkno`: Current block number for heap operations
- `offnum`: Current offset number for heap operations
- `phase`: Current phase of vacuum operation
- `verbose`: Whether verbose output is enabled

### Dead Items Management
- `dead_items`: TidStore containing TIDs of tuples to be deleted from indexes
- `dead_items_info`: Information about dead items storage

### Page Statistics
- `rel_pages`: Total number of pages in the relation
- `scanned_pages`: Number of pages actually examined
- `removed_pages`: Number of pages removed by truncation
- `frozen_pages`: Number of pages with newly frozen tuples
- `lpdead_item_pages`: Number of pages containing LP_DEAD items
- `missed_dead_pages`: Number of pages with missed dead tuples
- `nonempty_pages`: Last non-empty page plus one

### Tuple Statistics
- `new_rel_tuples`: New estimated total number of tuples
- `new_live_tuples`: New estimated number of live tuples
- `indstats`: Statistics results from index access methods

### Performance Counters
- `num_index_scans`: Number of index scans performed
- `tuples_deleted`: Number of tuples deleted from the table
- `tuples_frozen`: Number of newly frozen tuples
- `lpdead_items`: Number of items deleted from indexes
- `live_tuples`: Number of live tuples remaining
- `recently_dead_tuples`: Number of dead but not yet removable tuples
- `missed_dead_tuples`: Number of removable but not removed tuples

### Block Scanning State
- `current_block`: Last block returned by scanning
- `next_unskippable_block`: Next block that cannot be skipped
- `next_unskippable_allvis`: Visibility status of next unskippable block
- `next_unskippable_vmbuffer`: Buffer containing the visibility map bit

## Dependencies
- Functions called/Symbols referenced:
  - [BufferAccessStrategy](../B/BufferAccessStrategy.md)
  - [ParallelVacuumState](../P/ParallelVacuumState.md)
  - [VacuumCutoffs](../V/VacuumCutoffs.md)
  - [GlobalVisState](../G/GlobalVisState.md)
  - MultiXactId
  - [VacErrPhase](../V/VacErrPhase.md)
  - [TidStore](../T/TidStore.md)
  - [VacDeadItemsInfo](../V/VacDeadItemsInfo.md)
  - [IndexBulkDeleteResult](../I/IndexBulkDeleteResult.md)

- Called from (representative examples):
  - [heap_vacuum_rel](../h/heap_vacuum_rel.md)
  - [lazy_scan_heap](../l/lazy_scan_heap.md)
  - [heap_vac_scan_next_block](../h/heap_vac_scan_next_block.md)
  - [lazy_scan_prune](../l/lazy_scan_prune.md)
  - [lazy_vacuum_all_indexes](../l/lazy_vacuum_all_indexes.md)
  - [lazy_vacuum_heap_rel](../l/lazy_vacuum_heap_rel.md)

## Notes and Other Information
This structure is central to PostgreSQL's lazy vacuum implementation and is passed between most vacuum-related functions. It supports both parallel and serial vacuum operations, with special handling for shared memory allocation of dead_items and dead_items_info in parallel cases. The structure maintains comprehensive statistics that are used both for progress reporting and for updating system catalogs after vacuum completion.