# ExecPostprocessPlan

## Location
src/backend/executor/execMain.c: 1431 - 1476

## Overview
Provides plan nodes a final execution opportunity before shutdown, ensuring auxiliary ModifyTable nodes run to completion for predictable results regardless of main query fetch behavior.

## Definition


## Detailed Description
ExecPostprocessPlan performs final cleanup and completion tasks during executor shutdown. It specifically handles auxiliary ModifyTable nodes (stored in es_auxmodifytables) by running them to completion, ensuring all planned modifications are executed even if the main query didn't fetch all tuples. This is crucial for maintaining data consistency and predictable side effects, particularly when ModifyTable operations have been set up as auxiliary operations that might not be driven by the main query's tuple consumption.

## Parameters / Member Variables
- : The execution state containing auxiliary ModifyTable nodes and execution context

## Dependencies
- Functions called/Symbols referenced:
  - ForwardScanDirection (constant)
  - ResetPerTupleExprContext
  - ExecProcNode  
  - TupIsNull
- Called from (representative examples):
  - standard_ExecutorFinish (execMain.c:433)

## Notes and Other Information
- Forces forward scan direction to ensure consistent execution behavior
- Iterates through all auxiliary ModifyTable nodes in es_auxmodifytables list
- Runs each auxiliary node until it returns NULL (completion signal)  
- Resets per-tuple expression context between each tuple for proper memory management
- Essential for ensuring side effects occur predictably regardless of main query behavior
- Part of the executor's cleanup phase, called during ExecutorFinish
- Handles cases where auxiliary operations need completion independent of main query results
- Maintains transactional consistency by ensuring all planned modifications complete