# trivial_subqueryscan

## Location
[src/backend/optimizer/plan/setrefs.c:1464-1533](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/optimizer/plan/setrefs.c#L1464-L1533)

## Overview
Determines whether a SubqueryScan node can be safely eliminated from the plan tree by checking if it adds no meaningful processing beyond passing through its subplan's output.

## Definition

```c
bool
trivial_subqueryscan(SubqueryScan *plan)
```
## Detailed Description
 is a key optimization function that identifies SubqueryScan nodes that can be eliminated from the execution plan. A SubqueryScan is considered "trivial" when it serves no purpose other than wrapping its subplan - meaning it doesn't filter rows (no quals) and doesn't transform the output columns (targetlist just regurgitates subplan output).

The function implements caching to avoid repeated computation since it may be called multiple times during plan optimization phases. It uses the  field in the SubqueryScan node to track whether the determination has already been made.

The triviality check verifies:
1. No qualification conditions (plan.qual must be NIL)
2. Targetlist lengths match between parent and subplan
3. Each targetlist entry either:
   - Is a Var referencing the corresponding subplan output column in order
   - Is a Const that exactly equals the corresponding subplan constant expression
4. Junk status (resjunk) matches between corresponding entries

The function supports scenarios where targetlist entries are constants rather than just variables, which is important for set operations (see  for context).

## Parameters / Member Variables
- : The SubqueryScan node to evaluate for triviality

## Dependencies
- Functions called/Symbols referenced:
  - forboth: Macro for parallel iteration over two lists
  - [equal](../e/equal.md): Tests equality between expression nodes
  - SUBQUERY_SCAN_TRIVIAL/NONTRIVIAL/UNKNOWN: Status enumeration values for caching results
- Called from (representative examples):
  - [set_subqueryscan_references](../s/set_subqueryscan_references.md): Primary caller during plan reference adjustment
  - [mark_async_capable_plan](../m/mark_async_capable_plan.md): Called during Append plan creation to determine async capability

## Notes and Other Information
The caching mechanism is particularly important because the function may be called from  before plan finalization and again from  during reference adjustment. The comments explain why this is safe - the transformations that occur between these calls preserve the properties that affect triviality determination. This optimization is crucial for query performance as it can eliminate entire plan nodes from execution, reducing tuple passing overhead and simplifying the execution tree. The support for Const expressions in addition to Vars makes the function robust for set operations where constant folding may have occurred.

## Simplified Source

```c
bool
trivial_subqueryscan(SubqueryScan *plan)
{
    int attrno;

    // Use cached result if available
    if (plan->scanstatus == SUBQUERY_SCAN_TRIVIAL)
        return true;
    if (plan->scanstatus == SUBQUERY_SCAN_NONTRIVIAL)
        return false;

    // Mark as non-trivial initially (pessimistic assumption)
    plan->scanstatus = SUBQUERY_SCAN_NONTRIVIAL;

    // Cannot be trivial if there are qualification conditions
    if (plan->scan.plan.qual != NIL)
        return false;

    // Target lists must have same length
    if (list_length(plan->scan.plan.targetlist) !=
        list_length(plan->subplan->targetlist))
        return false;

    // Check each target list entry pair
    attrno = 1;
    forboth(lp, plan->scan.plan.targetlist, lc, plan->subplan->targetlist)
    {
        TargetEntry *parent_tle = lfirst(lp);
        TargetEntry *child_tle = lfirst(lc);

        // Junk status must match
        if (parent_tle->resjunk != child_tle->resjunk)
            return false;

        // Entry must be either a Var or matching Const
        if (parent_tle->expr && IsA(parent_tle->expr, Var))
        {
            Var *var = (Var *) parent_tle->expr;

            // Var must reference correct attribute in order
            if (var->varattno != attrno)
                return false;
        }
        else if (parent_tle->expr && IsA(parent_tle->expr, Const))
        {
            // Const must exactly match child expression
            if (!equal(parent_tle->expr, child_tle->expr))
                return false;
        }
        else
            return false;

        attrno++;
    }

    // All checks passed - mark as trivial
    plan->scanstatus = SUBQUERY_SCAN_TRIVIAL;
    return true;
}
```