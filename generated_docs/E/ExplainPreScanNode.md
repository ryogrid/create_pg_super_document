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

## Simplified Source

```c
static bool
ExplainPreScanNode(PlanState *planstate, Bitmapset **rels_used)
{
    Plan *plan = planstate->plan;

    // Add relation IDs based on node type
    switch (nodeTag(plan)) {
        case T_SeqScan:
        case T_IndexScan:
        case T_BitmapHeapScan:
        case T_SubqueryScan:
        case T_FunctionScan:
        case T_ValuesScan:
        case T_CteScan:
        // ... other simple scan types
            *rels_used = bms_add_member(*rels_used,
                                        ((Scan *) plan)->scanrelid);
            break;

        case T_ForeignScan:
            *rels_used = bms_add_members(*rels_used,
                                         ((ForeignScan *) plan)->fs_base_relids);
            break;

        case T_CustomScan:
            *rels_used = bms_add_members(*rels_used,
                                         ((CustomScan *) plan)->custom_relids);
            break;

        case T_ModifyTable:
            // Add nominal relation
            *rels_used = bms_add_member(*rels_used,
                                        ((ModifyTable *) plan)->nominalRelation);
            // Add excluded relation if present
            if (((ModifyTable *) plan)->exclRelRTI)
                *rels_used = bms_add_member(*rels_used,
                                            ((ModifyTable *) plan)->exclRelRTI);
            break;

        case T_Append:
            *rels_used = bms_add_members(*rels_used,
                                         ((Append *) plan)->apprelids);
            break;

        case T_MergeAppend:
            *rels_used = bms_add_members(*rels_used,
                                         ((MergeAppend *) plan)->apprelids);
            break;

        default:
            // No relations to add for other node types
            break;
    }

    // Recursively process child nodes
    return planstate_tree_walker(planstate, ExplainPreScanNode, rels_used);
}
```