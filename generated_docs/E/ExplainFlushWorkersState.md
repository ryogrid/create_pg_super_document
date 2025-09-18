# ExplainFlushWorkersState

## Location
src/backend/commands/explain.c: 4596 - 4625

## Overview
Prints per-worker information for the current node and then deallocates the ExplainWorkersState structure, finalizing the output of parallel execution details in query plans.

## Definition


## Detailed Description
This function is responsible for outputting worker-specific information that has been collected during parallel query execution and then cleaning up the associated memory. It iterates through all workers in the ExplainWorkersState, outputting the collected information for each initialized worker within proper XML/JSON grouping constructs. After outputting all worker information, it performs comprehensive cleanup by freeing all allocated memory including worker strings, initialization flags, saved states, and the main workers state structure itself.

The function ensures proper formatting by wrapping all worker output in "Workers" groups and individual worker data in "Worker" groups, maintaining consistency with PostgreSQL's explain output format across different output modes (text, XML, JSON).

## Parameters / Member Variables
- : Pointer to ExplainState containing the execution state and workers_state to be flushed and freed

## Dependencies
- Functions called/Symbols referenced:
  - ExplainState (struct type)
  - ExplainWorkersState (struct type)  
  - ExplainOpenGroup
  - ExplainCloseGroup
  - appendStringInfoString
  - pfree
- Called from (representative examples):
  - ExplainNode (at src/backend/commands/explain.c:2310)

## Notes and Other Information
- This is a static function, only accessible within the explain.c file
- The function assumes that workers_state has been properly initialized and contains valid worker data
- Memory cleanup is comprehensive, freeing worker_inited array, worker_str array, worker_state_save array, and the main wstate structure
- The function maintains proper nesting of explanation groups to ensure valid XML/JSON output formatting
- Only outputs information for workers that have been initialized (worker_inited[i] == true)