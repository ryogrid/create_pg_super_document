# convert_combining_aggrefs

## Location
[src/backend/optimizer/plan/setrefs.c:2552-2620](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/optimizer/plan/setrefs.c#L2552-L2620)

## Overview
Recursively scans an expression tree and converts Aggrefs to the proper intermediate form for combining aggregates in partial aggregation scenarios.

## Definition

```c
static Node *
convert_combining_aggrefs(Node *node, void *context)
```
## Detailed Description
This function transforms aggregate expressions (Aggrefs) in preparation for two-phase partial aggregation. It replaces each original Aggref with a parent-child structure where:
1. The child Aggref performs partial aggregation with the original arguments and filters
2. The parent Aggref performs final aggregation, taking the child Aggref as its single argument

The transformation is necessary for parallel aggregation scenarios where aggregates are computed partially by worker processes and then combined by the main process. The function creates two separate Aggref nodes: one marked as AGGSPLIT_INITIAL_SERIAL for the first phase and another marked as AGGSPLIT_FINAL_DESERIAL for the combining phase.

This step is performed during the setrefs phase rather than createplan phase to avoid breaking cross-plan-node-level matches, since modified Aggrefs would no longer be equal() to their original forms.

## Parameters / Member Variables
- : The expression tree node to process (typically containing Aggref nodes)
- : Context parameter passed through the expression tree traversal (unused in this function)

## Dependencies
- Functions called/Symbols referenced:
  - IsA (macro for type checking)
  - makeNode
  - memcpy
  - copyObject
  - [mark_partial_aggref](../m/mark_partial_aggref.md)
  - [makeTargetEntry](../m/makeTargetEntry.md)
  - list_make1
  - expression_tree_mutator
- Called from (representative examples):
  - fix_scan_list (src/backend/optimizer/plan/setrefs.c:166)
  - [set_plan_refs](../s/set_plan_refs.md) (src/backend/optimizer/plan/setrefs.c:953, 956)
  - [convert_combining_aggrefs](convert_combining_aggrefs.md) (recursive call at line 2603)

## Notes and Other Information
- Only supports aggregates without ORDER BY clauses (aggorder) or DISTINCT clauses (aggdistinct)
- The function assumes serialization is required for partial aggregation
- Creates a flat copy of the original Aggref to avoid modifying it in-place
- The child Aggref retains the original arguments and filter, while the parent Aggref gets a single argument (the child Aggref wrapped in a TargetEntry)
- This transformation enables efficient parallel execution of aggregate queries
- The function uses expression_tree_mutator for recursive traversal of the expression tree