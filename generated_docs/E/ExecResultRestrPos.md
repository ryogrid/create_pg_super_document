# ExecResultRestrPos

## Location
src/backend/executor/nodeResult.c: 161 - 179

## Overview
ExecResultRestrPos restores a previously marked position in a Result node, delegating the restore operation to its outer plan or throwing an error if no outer plan exists.

## Definition


## Detailed Description
ExecResultRestrPos implements the restore functionality for Result plan nodes as part of PostgreSQL's mark/restore position mechanism. This function is the counterpart to ExecResultMarkPos and is used to return to a previously marked execution position.

The function behavior depends on whether the Result node has an outer plan:
- **If outer plan exists**: Delegates the restore operation to the outer plan by calling ExecRestrPos on it
- **If no outer plan**: Throws an ERROR (not just a debug message like ExecResultMarkPos) because attempting to restore without a valid mark position is a serious execution error

The asymmetry between ExecResultMarkPos (which issues a debug message) and ExecResultRestrPos (which throws an error) reflects that marking an unsupported position is a warning-worthy event, while attempting to restore to an invalid position is a critical error that should halt execution.

## Parameters / Member Variables
- : The ResultState containing the execution state for this Result node

## Dependencies
- Functions called/Symbols referenced:
  - outerPlanState (to get the outer plan state)
  - ExecRestrPos (to restore position in the outer plan)
  - elog (to throw error when restore is not supported)
- Called from:
  - ExecRestrPos (general position restoration dispatcher in execAmi.c)
  - Declared in nodeResult.h

## Notes and Other Information
- Unlike ExecResultMarkPos which logs a debug message, this function throws an ERROR when called on Result nodes without outer plans
- The error behavior ensures that incorrect usage of mark/restore on constant Result nodes is caught immediately
- Mark/restore operations are typically coordinated, so an attempt to restore without a corresponding mark indicates a logical error in the execution plan
- This function is part of the executor's position management infrastructure used primarily by join algorithms