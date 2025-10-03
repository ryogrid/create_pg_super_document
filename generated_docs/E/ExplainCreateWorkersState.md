# ExplainCreateWorkersState

## Location
[src/backend/commands/explain.c:4481-4497](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/commands/explain.c#L4481-L4497)

## Overview
ExplainCreateWorkersState is a static function in PostgreSQL's explain module that creates a workspace structure for collecting and organizing per-worker data during parallel query execution explanation.

## Definition

```c
static ExplainWorkersState *
ExplainCreateWorkersState(int num_workers)
```
## Detailed Description
This function allocates and initializes an ExplainWorkersState structure that serves as a workspace for handling output from parallel workers during query plan explanation. The function creates separate buffers for each worker's output, which allows the explain system to collect worker-specific data independently and then merge it coherently into the main output stream. This design enables the generation of organized per-worker field groups even though the code that produces these fields is distributed across multiple locations in the explain module.

The workspace includes arrays for tracking initialization status, string buffers for worker output, and state saving capabilities. This infrastructure is essential for presenting parallel execution information in a structured and readable format.

## Parameters / Member Variables
- `num_workers`: The number of parallel workers that will be reporting execution statistics
## Dependencies
- Functions called/Symbols referenced:
  - [palloc](../p/palloc.md) (allocates memory for the main structure)
  - [palloc0](../p/palloc0.md) (allocates zero-initialized arrays for worker data)
- Called from:
  - [ExplainNode](ExplainNode.md) (when processing nodes with parallel workers)

## Notes and Other Information
- Creates arrays sized according to the number of workers: worker_inited (bool), worker_str (StringInfoData), and worker_state_save (int)
- The allocated structure supports temporary "set aside" buffering for worker output
- Works in conjunction with ExplainOpenSetAsideGroup and ExplainSaveGroup/ExplainRestoreGroup for formatting
- Essential for coherent presentation of parallel execution statistics
- Memory is allocated using PostgreSQL's memory context system
- File location: src/backend/commands/explain.c:4481-4497

## Simplified Source

```c
static ExplainWorkersState *
ExplainCreateWorkersState(int num_workers)
{
    // Allocate main workspace structure
    ExplainWorkersState *wstate = palloc(sizeof(ExplainWorkersState));

    // Set worker count
    wstate->num_workers = num_workers;

    // Allocate arrays for per-worker data
    wstate->worker_inited = palloc0(num_workers * sizeof(bool));
    wstate->worker_str = palloc0(num_workers * sizeof(StringInfoData));
    wstate->worker_state_save = palloc(num_workers * sizeof(int));

    return wstate;
}
```