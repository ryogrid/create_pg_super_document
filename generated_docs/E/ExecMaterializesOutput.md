# ExecMaterializesOutput

## Location
[src/backend/executor/execAmi.c:635-653](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/executor/execAmi.c#L635-L653)

## Overview
ExecMaterializesOutput determines whether a plan node type automatically materializes its output, which affects rescan performance characteristics and cost estimation.

## Definition

```c
bool
ExecMaterializesOutput(NodeTag plantype)
```
## Detailed Description
This function examines a plan node type and returns true if that node type automatically materializes its output, typically by storing tuples in a tuplestore or similar structure. Plans that materialize their output have the characteristic that a rescan operation (without parameter changes) will have zero startup cost and very low per-tuple cost, since the materialized data can be reused.

The function uses a switch statement to identify plan types that inherently materialize their output:
- Material nodes (explicitly designed for materialization)
- Function scans (may cache function results)
- Table function scans
- CTE (Common Table Expression) scans
- Named tuplestore scans
- Work table scans (used in recursive queries)
- Sort nodes (must materialize to perform sorting)

This information is crucial for the query optimizer when making cost-based decisions about plan selection and for determining whether certain optimization strategies (like nested loop joins) are beneficial.

## Parameters / Member Variables
- : A NodeTag enumeration value representing the type of plan node to examine

## Dependencies
- Functions called/Symbols referenced:
  - None (uses only built-in switch statement and NodeTag constants)
- Called from (representative examples):
  - [cost_subplan](../c/cost_subplan.md) (in optimizer cost estimation)
  - [match_unsorted_outer](../m/match_unsorted_outer.md) (in join path selection)
  - [build_subplan](../b/build_subplan.md) (in subquery planning)

## Notes and Other Information
- This function is used primarily by the query optimizer for cost estimation and plan selection
- The materialization characteristic significantly impacts rescan costs, making it important for nested loop join cost calculations
- The function only identifies node types that inherently materialize; it doesn't account for runtime materialization decisions
- [Plan](../P/Plan.md) types not listed in the switch statement are assumed to not materialize their output
- The function is critical for accurate cost estimation in scenarios involving multiple rescans of the same plan node