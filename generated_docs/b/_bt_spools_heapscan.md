# _bt_spools_heapscan

## Location
[src/backend/access/nbtree/nbtsort.c:363-514](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/access/nbtree/nbtsort.c#L363-L514)

## Overview
Manages the heap scanning phase of B-tree index construction, creating spool structures for temporary storage and coordinating parallel processing when applicable.

## Definition

```c
static double
_bt_spools_heapscan(Relation heap, Relation index, BTBuildState *buildstate,
					IndexInfo *indexInfo)
```
## Detailed Description
 is a critical function in B-tree index construction that handles the heap scanning phase. It performs several key operations:

1. **Spool Initialization**: Creates one or two BTSpool structures for temporary storage of index tuples during the build process. The primary spool handles live tuples, while an optional secondary spool manages dead tuples for unique indexes.

2. **Memory Management**: Allocates sort areas using  for the primary spool to optimize index creation performance. The secondary spool (if needed) uses the smaller  allocation since dead tuples are expected to be fewer.

3. **Parallel Processing Setup**: Detects and coordinates parallel index building when multiple worker processes are available. Sets up shared sort coordination structures for parallel tuplesort operations.

4. **Tuplesort Initialization**: Creates tuplesort states for both primary and secondary spools, configuring them for B-tree index tuple sorting with appropriate uniqueness and null handling settings.

5. **Heap Scanning**: Executes either serial or parallel heap scanning to read tuples from the source relation and populate the spool structures. Uses callback functions to process each tuple and add it to the appropriate spool.

6. **Progress Reporting**: Updates progress statistics for monitoring the index creation process, including tuple counts and scan progress.

7. **Cleanup Optimization**: Removes the secondary spool if no dead tuples were encountered during scanning, optimizing resource usage.

The function encapsulates all aspects of parallelism management, allowing the caller to simply call  when finished.

## Parameters
- : The source heap relation to scan for index tuples
- : The target index relation being constructed  
- : Build state structure to store spool references and build metadata
- : Index metadata including uniqueness, parallel worker count, and other properties

## Dependencies
- Functions called/Symbols referenced:
  -  - Initiates parallel processing setup
  -  - Creates tuplesort states for spools
  -  - Performs serial heap scanning
  -  - Performs parallel heap scanning  
  -  - Processes individual tuples during scan
  -  - Cleans up unused secondary spool
  - ,  - Progress reporting
  - , , ,  - Data structures
- Called from:
  -  - Main index construction function

## Notes and Other Information
- Uses  for primary spool allocation to speed index creation, while secondary spool uses smaller 
- Automatically detects when parallel processing is beneficial and coordinates multiple worker processes
- For unique indexes, maintains separate spools for live and dead tuples to optimize uniqueness checking
- Implements sophisticated memory management to ensure  represents an absolute high watermark regardless of parallelism
- Progress reporting integration allows monitoring of long-running index builds
- Returns the total number of heap tuples scanned for statistics and validation purposes

## Simplified Source

```c
static double
_bt_spools_heapscan(Relation heap, Relation index, BTBuildState *buildstate,
                    IndexInfo *indexInfo)
{
    BTSpool *btspool = (BTSpool *) palloc0(sizeof(BTSpool));
    SortCoordinate coordinate = NULL;
    double reltuples = 0;

    // Initialize primary spool
    btspool->heap = heap;
    btspool->index = index;
    btspool->isunique = indexInfo->ii_Unique;
    btspool->nulls_not_distinct = indexInfo->ii_NullsNotDistinct;
    buildstate->spool = btspool;

    // Report scan phase started
    pgstat_progress_update_param(PROGRESS_CREATEIDX_SUBPHASE,
                                 PROGRESS_BTREE_PHASE_INDEXBUILD_TABLESCAN);

    // Setup parallel processing if workers available
    if (indexInfo->ii_ParallelWorkers > 0)
        _bt_begin_parallel(buildstate, indexInfo->ii_Concurrent,
                           indexInfo->ii_ParallelWorkers);

    // Setup coordination for parallel workers
    if (buildstate->btleader) {
        coordinate = (SortCoordinate) palloc0(sizeof(SortCoordinateData));
        coordinate->isWorker = false;
        coordinate->nParticipants = buildstate->btleader->nparticipanttuplesorts;
        coordinate->sharedsort = buildstate->btleader->sharedsort;
    }

    // Initialize primary tuplesort
    buildstate->spool->sortstate =
        tuplesort_begin_index_btree(heap, index, buildstate->isunique,
                                    buildstate->nulls_not_distinct,
                                    maintenance_work_mem, coordinate,
                                    TUPLESORT_NONE);

    // Setup secondary spool for unique indexes (dead tuples)
    if (indexInfo->ii_Unique) {
        BTSpool *btspool2 = (BTSpool *) palloc0(sizeof(BTSpool));
        btspool2->heap = heap;
        btspool2->index = index;
        btspool2->isunique = false;
        buildstate->spool2 = btspool2;

        // Setup parallel coordination for second spool if needed
        SortCoordinate coordinate2 = NULL;
        if (buildstate->btleader) {
            coordinate2 = (SortCoordinate) palloc0(sizeof(SortCoordinateData));
            coordinate2->isWorker = false;
            coordinate2->nParticipants = buildstate->btleader->nparticipanttuplesorts;
            coordinate2->sharedsort = buildstate->btleader->sharedsort2;
        }

        buildstate->spool2->sortstate =
            tuplesort_begin_index_btree(heap, index, false, false, work_mem,
                                        coordinate2, TUPLESORT_NONE);
    }

    // Perform heap scan (serial or parallel)
    if (!buildstate->btleader)
        reltuples = table_index_build_scan(heap, index, indexInfo, true, true,
                                           _bt_build_callback, (void *) buildstate,
                                           NULL);
    else
        reltuples = _bt_parallel_heapscan(buildstate,
                                          &indexInfo->ii_BrokenHotChain);

    // Update progress reporting
    const int progress_index[] = {
        PROGRESS_CREATEIDX_TUPLES_TOTAL,
        PROGRESS_SCAN_BLOCKS_TOTAL,
        PROGRESS_SCAN_BLOCKS_DONE
    };
    const int64 progress_vals[] = {
        buildstate->indtuples,
        0, 0
    };
    pgstat_progress_update_multi_param(3, progress_index, progress_vals);

    // Cleanup unnecessary secondary spool if no dead tuples found
    if (buildstate->spool2 && !buildstate->havedead) {
        _bt_spooldestroy(buildstate->spool2);
        buildstate->spool2 = NULL;
    }

    return reltuples;
}
```