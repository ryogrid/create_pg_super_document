# ExecReScanGroup

## Location
[src/backend/executor/nodeGroup.c:235-249](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/executor/nodeGroup.c#L235-L249)

## Overview
ExecReScanGroup resets a Group plan node to its initial state for re-execution, clearing processed state and optionally rescanning its child plan node.

## Definition


## Detailed Description
ExecReScanGroup implements the rescan operation for Group plan nodes, which is necessary when a Group node needs to be re-executed from the beginning. This typically occurs in nested loop joins where the inner side (containing the Group node) must be reset and re-executed for each outer tuple.

The rescan process involves several key steps:
1. **State Reset**: Sets grp_done flag to false to indicate the node is ready for execution
2. **Tuple State Cleanup**: Clears the scan tuple slot that holds the first tuple of each group
3. **Child Rescan**: Conditionally rescans the child plan node based on parameter change detection

The function includes an optimization where it only rescans the child node if no parameters have changed (chgParam is NULL). If parameters have changed, the child will be automatically rescanned when ExecProcNode is first called, avoiding redundant work.

## Parameters / Member Variables
- : The GroupState node to be rescanned and reset for re-execution

## Dependencies
- Functions called/Symbols referenced:
  - [GroupState](../G/GroupState.md) (node parameter type)
  - [PlanState](../P/PlanState.md) (child plan reference)
  - outerPlanState (child plan access macro)
  - ExecClearTuple (tuple slot cleanup)
  - [ExecReScan](ExecReScan.md) (recursive child rescan)
- Called from (representative examples):
  - [ExecReScan](ExecReScan.md) (during plan node rescan operations)

## Notes and Other Information
- Essential for nested loop joins and other scenarios requiring plan re-execution
- The grp_done flag controls whether execution continues or terminates
- Clearing the scan tuple slot is crucial as it holds the group comparison tuple
- Parameter change optimization (chgParam check) prevents unnecessary rescans
- The scan tuple slot clearing ensures group detection starts fresh on rescan
- Part of PostgreSQL's standard node interface supporting parameterized and repeated execution