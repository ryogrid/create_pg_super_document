# ExplainWorkersState

## Location
[src/include/commands/explain.h:35-42](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/include/commands/explain.h#L35-L42)

## Overview
A structure that manages per-worker output state for parallel query execution plans in PostgreSQL EXPLAIN output, allowing separate collection and formatting of execution statistics from each worker process.

## Definition
```c
typedef struct ExplainWorkersState
{
    int         num_workers;        /* # of worker processes the plan used */
    bool       *worker_inited;      /* per-worker state-initialized flags */
    StringInfoData *worker_str;     /* per-worker transient output buffers */
    int        *worker_state_save;  /* per-worker grouping state save areas */
    StringInfo  prev_str;           /* saved output buffer while redirecting */
} ExplainWorkersState;
```

## Detailed Description
ExplainWorkersState is a workspace structure used during EXPLAIN command execution to manage output formatting for parallel query plans. When PostgreSQL executes a query using parallel workers, each worker process generates its own execution statistics (timing, buffer usage, etc.). This structure provides the infrastructure to collect, format, and merge worker-specific output into a coherent presentation.

The structure supports a "set-aside" buffering mechanism where output for each worker is temporarily redirected to separate buffers, allowing the EXPLAIN code to generate well-formatted field groups for each worker even though the field-generating code is distributed across multiple functions. This ensures that worker-specific data appears in a logical, grouped format rather than being interleaved randomly.

## Parameters / Member Variables
- `num_workers`: The total number of worker processes that participated in executing the query plan
- `worker_inited`: Array of boolean flags indicating whether each worker's output buffer has been initialized and formatted
- `worker_str`: Array of StringInfoData structures, one per worker, serving as temporary output buffers for worker-specific data
- `worker_state_save`: Array of integers storing saved formatting state for each worker, used when resuming output after switching between workers
- `prev_str`: Pointer to the main output buffer, saved when redirecting output to a worker-specific buffer

## Dependencies
- Functions called/Symbols referenced:
  - [StringInfoData](../S/StringInfoData.md) (PostgreSQL string buffer type)
  - StringInfo (pointer to StringInfoData)

- Called from (representative examples):
  - [ExplainCreateWorkersState](ExplainCreateWorkersState.md) (creates and initializes the structure)
  - [ExplainOpenWorker](ExplainOpenWorker.md) (switches output to a specific worker buffer)
  - [ExplainCloseWorker](ExplainCloseWorker.md) (saves worker state and switches back to main buffer)
  - [ExplainFlushWorkersState](ExplainFlushWorkersState.md) (merges all worker outputs into main output)

## Notes and Other Information
- This structure is only allocated and used when explaining parallel query plans (when num_workers > 0)
- The structure supports both TEXT and non-TEXT output formats, with special handling for TEXT format worker prefixes
- Memory for the arrays is allocated dynamically based on the number of workers
- The "set-aside" mechanism allows complex, multi-step field generation while maintaining clean output grouping
- Used in conjunction with ExplainState structure, which contains a pointer to ExplainWorkersState in its workers_state field
- The structure enables proper formatting of worker-specific metrics like execution time, buffer hits, and other statistics in EXPLAIN ANALYZE output