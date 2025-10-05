# _bt_leader_participate_as_worker

## Location
[src/backend/access/nbtree/nbtsort.c:1687-1739](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/access/nbtree/nbtsort.c#L1687-L1739)

## Overview
Enables the leader process to participate as a worker during parallel B-tree index construction, performing the same scanning and sorting work as dedicated worker processes.

## Definition

```c
static void
_bt_leader_participate_as_worker(BTBuildState *buildstate)
```
## Detailed Description
This function transforms the leader process into an active participant in the parallel B-tree index build operation. Rather than just coordinating workers, the leader performs actual index construction work by setting up its own private spool structures and participating in the parallel heap scan and tuple sorting process.

The function creates private spool structures for the leader that mirror the worker setup:
- Primary spool for regular index tuple processing
- Secondary spool (for unique indexes) to handle potential duplicate detection and resolution

The leader allocates its share of maintenance_work_mem based on the actual number of participating tuple sorts, which may differ from the initially requested number if some workers failed to launch. This ensures optimal memory utilization across all participants.

Key responsibilities include:
- Setting up private BTSpool structures for leader's work portion
- Initializing secondary spool for unique index processing if needed
- Calculating appropriate memory allocation per participant
- Calling the common parallel scan and sort function
- Optional performance statistics reporting in debug builds

## Parameters / Member Variables
- `*buildstate`: Main B-tree build state containing shared leader information and original spool configuration
## Dependencies
- Functions called/Symbols referenced:
  - [palloc0](../p/palloc0.md): Allocate zero-initialized memory for spool structures
  - [_bt_parallel_scan_and_sort](_bt_parallel_scan_and_sort.md): Core parallel scanning and sorting function
  - [ShowUsage](../S/ShowUsage.md)/ResetUsage: Debug performance statistics (ifdef BTREE_BUILD_STATS)
  - [BTSpool](../B/BTSpool.md): Private spool structure for tuple processing
  - [BTLeader](../B/BTLeader.md): Leader state containing shared context and worker coordination
- Called from (representative examples):
  - [_bt_begin_parallel](_bt_begin_parallel.md): Main parallel setup function when leader participation is enabled

## Notes and Other Information
- Only called when leader participation is enabled (not disabled by DISABLE_LEADER_PARTICIPATION)
- Memory allocation accounts for actual number of launched workers, not requested count
- Secondary spool setup depends on index uniqueness constraint requirements
- Includes conditional debug statistics reporting for performance analysis
- Leader performs identical work to dedicated workers, improving overall throughput
- Memory distribution ensures fairness when fewer workers than requested are available

## Simplified Source

```c
static void
_bt_leader_participate_as_worker(BTBuildState *buildstate)
{
    BTLeader *btleader = buildstate->btleader;
    BTSpool *leaderworker;
    BTSpool *leaderworker2;
    int sortmem;

    // Set up primary spool for leader as worker
    leaderworker = (BTSpool *) palloc0(sizeof(BTSpool));
    leaderworker->heap = buildstate->spool->heap;
    leaderworker->index = buildstate->spool->index;
    leaderworker->isunique = buildstate->spool->isunique;
    leaderworker->nulls_not_distinct = buildstate->spool->nulls_not_distinct;

    // Set up secondary spool if needed for unique indexes
    if (!btleader->btshared->isunique) {
        leaderworker2 = NULL;
    } else {
        leaderworker2 = (BTSpool *) palloc0(sizeof(BTSpool));
        leaderworker2->heap = leaderworker->heap;
        leaderworker2->index = leaderworker->index;
        leaderworker2->isunique = false;
    }

    // Calculate memory per participant based on actual worker count
    sortmem = maintenance_work_mem / btleader->nparticipanttuplesorts;

    // Perform the same work as dedicated workers
    _bt_parallel_scan_and_sort(leaderworker, leaderworker2, btleader->btshared,
                               btleader->sharedsort, btleader->sharedsort2,
                               sortmem, true);
}
```