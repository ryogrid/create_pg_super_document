# UpdateChangedParamSet

## Location
src/backend/executor/execUtils.c: 844 - 869

## Overview
Updates a plan node's chgParam set with newly changed parameters that the node actually depends on, filtering out irrelevant parameter changes.

## Definition
```c
void UpdateChangedParamSet(PlanState *node, Bitmapset *newchg)
```

## Detailed Description
This function is used during query execution to track parameter changes that affect plan nodes. When parameters change (typically due to nested loop joins or correlated subqueries), this function updates a plan node's chgParam (changed parameters) set. However, it only includes parameters that the node actually depends on (as indicated by the node's allParam set), filtering out parameter changes that are irrelevant to this particular node. This optimization prevents unnecessary work during plan re-execution and ensures that nodes only respond to parameter changes that actually affect their operation.

## Parameters / Member Variables
- `node`: Plan state node whose changed parameter set needs updating
- `newchg`: Bitmapset of newly changed parameter IDs

## Dependencies
- Functions called/Symbols referenced:
  - [bms_intersect](../b/bms_intersect.md)
  - [bms_join](../b/bms_join.md)
- Called from (representative examples):
  - [ExecReScan](../E/ExecReScan.md)
  - [ExecReScanAppend](../E/ExecReScanAppend.md)
  - [ExecReScanBitmapAnd](../E/ExecReScanBitmapAnd.md)
  - [ExecReScanBitmapOr](../E/ExecReScanBitmapOr.md)
  - ExecReScanMergeAppend
  - [ExecReScanSubqueryScan](../E/ExecReScanSubqueryScan.md)

## Notes and Other Information
- Used primarily during plan re-scanning operations when parameters change
- The intersection with allParam ensures only relevant parameter changes are tracked
- Essential for parameterized plans where subplans depend on outer plan parameters
- Helps optimize nested loop joins and correlated subqueries by avoiding unnecessary re-execution
- The chgParam set is used by plan nodes to determine what work needs to be redone during rescans