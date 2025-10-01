# ExplainFlushWorkersState

## Location
[src/backend/commands/explain.c:4596-4625](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/commands/explain.c#L4596-L4625)

## Overview
Prints per-worker information for the current node and then deallocates the ExplainWorkersState structure, finalizing the output of parallel execution details in query plans.

## Definition

```c
static void
ExplainFlushWorkersState(ExplainState *es)
```
## Detailed Description
This function is responsible for outputting worker-specific information that has been collected during parallel query execution and then cleaning up the associated memory. It iterates through all workers in the ExplainWorkersState, outputting the collected information for each initialized worker within proper XML/JSON grouping constructs. After outputting all worker information, it performs comprehensive cleanup by freeing all allocated memory including worker strings, initialization flags, saved states, and the main workers state structure itself.

The function ensures proper formatting by wrapping all worker output in "Workers" groups and individual worker data in "Worker" groups, maintaining consistency with PostgreSQL's explain output format across different output modes (text, XML, JSON).

## Parameters / Member Variables
- : Pointer to ExplainState containing the execution state and workers_state to be flushed and freed

## Dependencies
- Functions called/Symbols referenced:
  - [ExplainState](ExplainState.md) (struct type)
  - [ExplainWorkersState](ExplainWorkersState.md) (struct type)  
  - [ExplainOpenGroup](ExplainOpenGroup.md)
  - [ExplainCloseGroup](ExplainCloseGroup.md)
  - [appendStringInfoString](../a/appendStringInfoString.md)
  - [pfree](../p/pfree.md)
- Called from (representative examples):
  - [ExplainNode](ExplainNode.md) (at src/backend/commands/explain.c:2310)

## Notes and Other Information
- This is a static function, only accessible within the explain.c file
- The function assumes that workers_state has been properly initialized and contains valid worker data
- Memory cleanup is comprehensive, freeing worker_inited array, worker_str array, worker_state_save array, and the main wstate structure
- The function maintains proper nesting of explanation groups to ensure valid XML/JSON output formatting
- Only outputs information for workers that have been initialized (worker_inited[i] == true)

## Simplified Source

```c
static void
ExplainFlushWorkersState(ExplainState *es)
{
    ExplainWorkersState *wstate = es->workers_state;

    // Begin workers output group
    ExplainOpenGroup("Workers", "Workers", false, es);

    // Output each initialized worker's data
    for (int i = 0; i < wstate->num_workers; i++) {
        if (wstate->worker_inited[i]) {
            // Open worker group and output collected data
            ExplainOpenGroup("Worker", NULL, true, es);
            appendStringInfoString(es->str, wstate->worker_str[i].data);
            ExplainCloseGroup("Worker", NULL, true, es);

            // Free worker's string buffer
            pfree(wstate->worker_str[i].data);
        }
    }

    // Close workers group
    ExplainCloseGroup("Workers", "Workers", false, es);

    // Clean up all allocated memory
    pfree(wstate->worker_inited);
    pfree(wstate->worker_str);
    pfree(wstate->worker_state_save);
    pfree(wstate);
}
```