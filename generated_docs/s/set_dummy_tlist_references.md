# set_dummy_tlist_references

## Location
[src/backend/optimizer/plan/setrefs.c:2621-2687](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/optimizer/plan/setrefs.c#L2621-L2687)

## Overview
Replaces the targetlist of an upper-level plan node with a simple list of OUTER_VAR references to its child plan node.

## Definition

```c
static void
set_dummy_tlist_references(Plan *plan, int rtoffset)
```
## Detailed Description
This function is used for plan node types like Sort, Append, and other operations that don't actually evaluate their targetlists during execution. While the executor ignores the targetlist content for these nodes, EXPLAIN and other introspection tools need the targetlist to be realistic and properly reference the underlying data sources.

The function transforms each target entry in the plan's targetlist to reference the corresponding output from the child plan using OUTER_VAR references. It preserves constants as constants rather than converting them to variable references, both for cleaner EXPLAIN output and to avoid confusing the executor. For variables that have syntactic information (varnosyn), it adjusts the relation numbers by the rtoffset parameter to maintain proper referencing in the context of subqueries.

## Parameters / Member Variables
- : The Plan node whose targetlist needs to be converted to dummy references
- : Offset to be added to relation numbers for proper referencing in subquery contexts

## Dependencies
- Functions called/Symbols referenced:
  - lfirst (list iteration macro)
  - IsA (type checking macro)
  - [lappend](../l/lappend.md)
  - [makeVar](../m/makeVar.md)
  - OUTER_VAR (special varno for referencing outer plan)
  - [exprType](../e/exprType.md)
  - [exprTypmod](../e/exprTypmod.md)
  - [exprCollation](../e/exprCollation.md)
  - [flatCopyTargetEntry](../f/flatCopyTargetEntry.md)
- Called from (representative examples):
  - fix_scan_list (src/backend/optimizer/plan/setrefs.c:167)
  - [set_plan_refs](set_plan_refs.md) (multiple calls for different plan types)
  - [set_append_references](set_append_references.md) (src/backend/optimizer/plan/setrefs.c:1781)
  - [set_mergeappend_references](set_mergeappend_references.md) (src/backend/optimizer/plan/setrefs.c:1863)
  - [set_hash_references](set_hash_references.md) (src/backend/optimizer/plan/setrefs.c:1923)

## Notes and Other Information
- Specifically designed for plan nodes that don't evaluate their targetlists (Sort, Append, etc.)
- Preserves constants as constants rather than converting to variable references for cleaner EXPLAIN output
- Handles syntactic variable information (varnosyn, varattnosyn) properly for subquery contexts
- The qual expressions of the plan are not modified by this function
- Could potentially use set_upper_references() but that fails for Append nodes due to lack of lefttree subplan
- Single-purpose implementation provides better performance than more general alternatives

## Simplified Source

```c
static void
set_dummy_tlist_references(Plan *plan, int rtoffset) {
    List *output_targetlist = NIL;

    foreach(l, plan->targetlist) {
        TargetEntry *tle = (TargetEntry *) lfirst(l);
        Var *oldvar = (Var *) tle->expr;

        // Keep constants as constants for cleaner EXPLAIN output
        if (IsA(oldvar, Const)) {
            output_targetlist = lappend(output_targetlist, tle);
            continue;
        }

        // Create new OUTER_VAR reference to child plan output
        Var *newvar = makeVar(OUTER_VAR, tle->resno,
                             exprType((Node *) oldvar),
                             exprTypmod((Node *) oldvar),
                             exprCollation((Node *) oldvar), 0);

        // Handle syntactic variable information for subqueries
        if (IsA(oldvar, Var) && oldvar->varnosyn > 0) {
            newvar->varnosyn = oldvar->varnosyn + rtoffset;
            newvar->varattnosyn = oldvar->varattnosyn;
        } else {
            newvar->varnosyn = 0;
            newvar->varattnosyn = 0;
        }

        // Create new target entry with OUTER_VAR reference
        tle = flatCopyTargetEntry(tle);
        tle->expr = (Expr *) newvar;
        output_targetlist = lappend(output_targetlist, tle);
    }

    plan->targetlist = output_targetlist;
}
```