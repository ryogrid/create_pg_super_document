# ExecCustomRestrPos

## Location
src/backend/executor/nodeCustom.c: 150 - 160

## Overview
Restores the position of a custom scan node to a previously marked position using the custom scan method's RestrPosCustomScan callback.

## Definition
```c
void ExecCustomRestrPos(CustomScanState *node)
```

## Detailed Description
ExecCustomRestrPos is responsible for restoring a custom scan node to a position that was previously marked using ExecCustomMarkPos. This function is part of PostgreSQL's executor framework that allows custom scan providers to implement position-based scan operations. The function delegates the actual restoration work to the custom scan method's RestrPosCustomScan callback function. If the custom scan provider does not support position restoration (i.e., RestrPosCustomScan is NULL), the function raises an error indicating that the feature is not supported.

## Parameters / Member Variables
- `node`: A pointer to the CustomScanState structure representing the custom scan node whose position should be restored

## Dependencies
- Functions called/Symbols referenced:
  - CustomScanState (structure type)
  - ereport (error reporting function)
  - errcode (error code macro)
  - errmsg (error message macro)
- Called from (representative examples):
  - ExecRestrPos (general position restoration dispatcher)

## Notes and Other Information
- This function is part of the custom scan API that allows extension developers to implement their own scan methods
- The function will raise an ERROR with code ERRCODE_FEATURE_NOT_SUPPORTED if the custom scan provider doesn't implement the RestrPosCustomScan method
- Position restoration is typically used in conjunction with position marking for operations that need to backtrack through scan results
- The error message incorrectly refers to "MarkPos" in the implementation, but the function is actually for restoring position