# rowcomparesel

## Location
src/backend/utils/adt/selfuncs.c: 2206 - 2272

## Overview
Estimates the selectivity of RowCompareExpr nodes by focusing on the first (highest order) column, effectively treating complex row comparisons as simple operator expressions for planning purposes.

## Definition


## Detailed Description
This function provides selectivity estimation for row comparison expressions (e.g., ). The implementation uses a simplified approach that considers only the leading columns of the row comparison, making it equivalent to an ordinary OpExpr evaluation. While this approach could theoretically be refined by considering additional columns, the current implementation acknowledges that significant improvements would require multi-column statistics that are typically not available.

The function determines whether the clause should be treated as a join clause or restriction clause based on the calling context and the number of relations involved, then delegates to the appropriate selectivity estimation function ( or ).

## Parameters / Member Variables
- : PlannerInfo structure containing planner state and context
- : RowCompareExpr node representing the row comparison expression to analyze
- : Relation ID when forcing restriction mode; 0 for normal operation
- : Type of join operation (used for join selectivity estimation)
- : Special join information structure; NULL for restriction clauses

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
- Join vs. restriction clause determination follows similar logic to clausesel.c's treat_as_join_clause()
- When varRelid is non-zero, the function is forced into restriction mode (e.g., for inner indexscan qualifiers)
- The approach trades accuracy for simplicity, acknowledging that better estimates would require complex multi-column statistics
- Returns a Selectivity value (double between 0.0 and 1.0) representing the estimated fraction of rows that will satisfy the condition