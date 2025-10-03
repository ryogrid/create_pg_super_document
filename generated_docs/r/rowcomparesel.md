# rowcomparesel

## Location
[src/backend/utils/adt/selfuncs.c:2206-2272](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/selfuncs.c#L2206-L2272)

## Overview
Estimates the selectivity of RowCompareExpr nodes by focusing on the first (highest order) column, effectively treating complex row comparisons as simple operator expressions for planning purposes.

## Definition

```c
Selectivity
rowcomparesel(PlannerInfo *root,
			  RowCompareExpr *clause,
			  int varRelid, JoinType jointype, SpecialJoinInfo *sjinfo)
```
## Detailed Description
This function provides selectivity estimation for row comparison expressions (e.g., ). The implementation uses a simplified approach that considers only the leading columns of the row comparison, making it equivalent to an ordinary OpExpr evaluation. While this approach could theoretically be refined by considering additional columns, the current implementation acknowledges that significant improvements would require multi-column statistics that are typically not available.

The function determines whether the clause should be treated as a join clause or restriction clause based on the calling context and the number of relations involved, then delegates to the appropriate selectivity estimation function ( or ).

## Parameters / Member Variables
- `*root`: PlannerInfo structure containing planner state and context
- `*clause`: RowCompareExpr node representing the row comparison expression to analyze
- `varRelid`: Relation ID when forcing restriction mode; 0 for normal operation
- `jointype`: Type of join operation (used for join selectivity estimation)
- `*sjinfo`: Special join information structure; NULL for restriction clauses
## Dependencies
- Functions called/Symbols referenced:
  - linitial_oid
  - list_make2
  - [NumRelids](../N/NumRelids.md)
  - [join_selectivity](../j/join_selectivity.md)
  - [restriction_selectivity](restriction_selectivity.md)
- Called from (representative examples):
  - [clause_selectivity_ext](../c/clause_selectivity_ext.md)

## Notes and Other Information
- The function intentionally simplifies row comparisons to single-column comparisons for estimation purposes
- [Join](../J/Join.md) vs. restriction clause determination follows similar logic to clausesel.c's treat_as_join_clause()
- When varRelid is non-zero, the function is forced into restriction mode (e.g., for inner indexscan qualifiers)
- The approach trades accuracy for simplicity, acknowledging that better estimates would require complex multi-column statistics
- Returns a Selectivity value (double between 0.0 and 1.0) representing the estimated fraction of rows that will satisfy the condition

## Simplified Source

```c
Selectivity
rowcomparesel(PlannerInfo *root, RowCompareExpr *clause,
              int varRelid, JoinType jointype, SpecialJoinInfo *sjinfo)
{
    Selectivity s1;
    Oid opno = linitial_oid(clause->opnos);
    Oid inputcollid = linitial_oid(clause->inputcollids);
    List *opargs;
    bool is_join_clause;

    // Build equivalent arg list for single operator (first column only)
    opargs = list_make2(linitial(clause->largs), linitial(clause->rargs));

    // Determine if this should be treated as join or restriction clause
    if (varRelid != 0) {
        // Forced restriction mode
        is_join_clause = false;
    } else if (sjinfo == NULL) {
        // Scan-level evaluation
        is_join_clause = false;
    } else {
        // Join-level: check if multiple relations involved
        is_join_clause = (NumRelids(root, (Node *) opargs) > 1);
    }

    // Delegate to appropriate selectivity estimator
    if (is_join_clause) {
        s1 = join_selectivity(root, opno, opargs, inputcollid,
                             jointype, sjinfo);
    } else {
        s1 = restriction_selectivity(root, opno, opargs, inputcollid,
                                    varRelid);
    }

    return s1;
}
```