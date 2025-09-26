# ExplainPreScanNode

## Location
[src/backend/commands/explain.c:1292-1366](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/commands/explain.c#L1292-L1366)

## Overview
Prescans the plan state tree to identify which range table entries (RTEs) are referenced by the query execution plan.

## Definition
```c
static bool ExplainPreScanNode(PlanState *planstate, Bitmapset **rels_used)
```

## Detailed Description
The `ExplainPreScanNode` function performs a preliminary scan of the entire plan state tree to determine which range table entries (RTEs) are actually referenced during query execution. This information is crucial for the explain functionality to determine which relations need aliases in the output and prevents confusing assignment of un-suffixed aliases to RTEs that never appear in the EXPLAIN output (such as inheritance parents).

The function uses a switch statement to handle different plan node types and extracts relation IDs based on the specific characteristics of each node type. It recursively traverses the entire plan tree using `planstate_tree_walker` to ensure all referenced relations are identified.

## Parameters / Member Variables
- `planstate`: Pointer to the current PlanState node being examined
- `rels_used`: Pointer to a Bitmapset that accumulates the relation IDs of all referenced RTEs

## Dependencies
- Functions called/Symbols referenced:
  - nodeTag
  - [bms_add_member](../b/bms_add_member.md)
  - [bms_add_members](../b/bms_add_members.md)
  - planstate_tree_walker
  - [Scan](../S/Scan.md) (cast)
  - [ForeignScan](../F/ForeignScan.md) (cast)
  - [CustomScan](../C/CustomScan.md) (cast)
  - [ModifyTable](../M/ModifyTable.md) (cast)
  - [Append](../A/Append.md) (cast)
  - [MergeAppend](../M/MergeAppend.md) (cast)
- Called from (representative examples):
  - [ExplainPrintPlan](ExplainPrintPlan.md)
  - [ExplainPreScanNode](ExplainPreScanNode.md) (recursive)

## Notes and Other Information
- Returns a boolean value (though the return value from planstate_tree_walker is typically used for control flow)
- Handles various scan types including sequential, index, bitmap heap, foreign, and custom scans
- For ModifyTable operations, it considers both the nominal relation and any excluded relation RTI
- The function is recursive, calling itself through the planstate_tree_walker mechanism
- Essential for proper alias generation in EXPLAIN output to avoid confusion
- Uses PostgreSQL's bitmapset data structure to efficiently track relation usage