# ExplainCreateWorkersState

## Location
src/backend/commands/explain.c: 4481 - 4497

## Overview
ExplainCreateWorkersState is a static function in PostgreSQL's explain module that creates a workspace structure for collecting and organizing per-worker data during parallel query execution explanation.

## Definition


## Detailed Description
This function allocates and initializes an ExplainWorkersState structure that serves as a workspace for handling output from parallel workers during query plan explanation. The function creates separate buffers for each worker's output, which allows the explain system to collect worker-specific data independently and then merge it coherently into the main output stream. This design enables the generation of organized per-worker field groups even though the code that produces these fields is distributed across multiple locations in the explain module.

The workspace includes arrays for tracking initialization status, string buffers for worker output, and state saving capabilities. This infrastructure is essential for presenting parallel execution information in a structured and readable format.

## Parameters / Member Variables
- : The number of parallel workers that will be reporting execution statistics

## Dependencies
- Functions called/Symbols referenced:
  - palloc (allocates memory for the main structure)
  - palloc0 (allocates zero-initialized arrays for worker data)
- Called from:
  - ExplainNode (when processing nodes with parallel workers)

## Notes and Other Information
- Creates arrays sized according to the number of workers: worker_inited (bool), worker_str (StringInfoData), and worker_state_save (int)
- The allocated structure supports temporary "set aside" buffering for worker output
- Works in conjunction with ExplainOpenSetAsideGroup and ExplainSaveGroup/ExplainRestoreGroup for formatting
- Essential for coherent presentation of parallel execution statistics
- Memory is allocated using PostgreSQL's memory context system
- File location: src/backend/commands/explain.c:4481-4497