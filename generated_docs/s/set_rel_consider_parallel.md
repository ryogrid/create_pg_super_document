# set_rel_consider_parallel

## Location
src/backend/optimizer/path/allpaths.c: 589 - 763

## Overview
Determines whether a relation can be safely scanned within a parallel worker by evaluating various parallel safety constraints and setting the consider_parallel flag accordingly.

## Definition


## Detailed Description
This function performs comprehensive parallel safety analysis for base relations in PostgreSQL's query optimizer. It evaluates whether a relation can be safely accessed from within parallel workers by checking numerous constraints including relation type, temporary table restrictions, function safety, FDW capabilities, and expression safety. The function sets the  flag to true only when all safety requirements are met.

The function uses a cautious approach - it starts with the assumption that parallel access is not safe and returns early whenever any unsafe condition is detected. It performs different checks based on the relation type (rtekind), including special handling for temporary tables (which cannot be accessed by workers), table sampling functions, foreign tables (requiring FDW cooperation), subqueries with LIMIT/OFFSET, and various other relation types.

After relation-type-specific checks, the function validates that all base restriction clauses and output expressions are parallel-safe. Only when all these checks pass does it set the  flag to true, enabling the relation for potential parallel execution paths.

## Parameters / Member Variables
- : PlannerInfo structure containing global optimizer state and parallelism configuration
- : RelOptInfo structure representing the relation being evaluated for parallel safety
- : RangeTblEntry containing parse tree information about the relation

## Dependencies
- Functions called/Symbols referenced:
  - get_rel_persistence (checks if relation is temporary)
  - func_parallel (determines function's parallel safety)
  - is_parallel_safe (validates expressions for parallel execution)
  - limit_needed (checks if subquery has LIMIT/OFFSET)
  - IS_SIMPLE_REL (macro to validate relation type)
- Called from:
  - set_base_rel_sizes (main size estimation phase)
  - set_append_rel_size (inheritance relation handling)

## Notes and Other Information
- This is a static function within allpaths.c that's part of the parallel query planning infrastructure
- The function assumes parallelism is globally enabled (checked via root->glob->parallelModeOK)
- Temporary tables are explicitly excluded because workers cannot access the leader's temporary buffers
- Foreign tables require explicit FDW support through the IsForeignScanParallelSafe callback
- Subqueries with LIMIT/OFFSET are excluded due to potential non-deterministic row ordering
- CTE and named tuple store relations cannot be parallelized due to sharing limitations
- The function uses early return strategy - any unsafe condition immediately disqualifies parallel access
- Table functions (RTE_TABLEFUNC) are currently not supported for parallel execution
- The consider_parallel flag enables subsequent parallel path generation but doesn't guarantee parallel execution