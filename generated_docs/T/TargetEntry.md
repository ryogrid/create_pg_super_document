# TargetEntry

## Location
src/include/nodes/primnodes.h: 2186 - 2203

## Overview
A fundamental node representing entries in query target lists, used throughout PostgreSQL's query processing system to describe expressions and their metadata in SELECT, INSERT, and UPDATE operations.

## Definition


## Detailed Description
TargetEntry is one of PostgreSQL's most fundamental structures, representing individual items in query target lists. While technically not an expression node (since it cannot be evaluated by ExecEvalExpr), it is treated as one for convenience in processing entire target lists as expression trees.

The behavior of TargetEntry varies depending on the query type:
- **SELECT**: resno equals the item's ordinal position (1-based)
- **INSERT**: resno represents the destination column's attribute number, may have gaps or be out-of-order
- **UPDATE**: resno may have duplicates initially (e.g., array element assignments), but gets normalized during planning

The structure supports complex query processing through several key mechanisms:
- Source tracking via resorigtbl/resorigcol for simple column references
- Sort/group operation integration through ressortgroupref
- Working column support through the resjunk flag
- Column naming for frontend communication via resname

## Parameters / Member Variables
- : Base expression node structure (inherited from Expr)
- : The expression to be evaluated for this target entry
- : Result attribute number - ordinal position in SELECT, destination column in INSERT/UPDATE
- : Column name for output, critical for top-level SELECT, may be NULL in internal nodes
- : Non-zero identifier linking this entry to ORDER BY/GROUP BY/DISTINCT clauses
- : OID of the source table if this is a simple column reference, zero otherwise
- : Column number in the source table if this is a simple column reference, zero otherwise
- : True for working columns (sort keys, etc.) that should be removed from final output

## Dependencies
- Functions called/Symbols referenced:
  - AttrNumber (for column numbering)
  - [Expr](../E/Expr.md) (base expression structure)
- Called from (representative examples):
  - Used extensively throughout query processing
  - Target list construction and manipulation
  - Expression evaluation and planning
  - [Result](../R/Result.md) set formatting

## Notes and Other Information
- Core component of PostgreSQL's target list representation
- The resno numbering gets normalized during planning but may be irregular before that
- Resjunk entries must have unique resnos that don't conflict with regular columns
- Resjunk columns are typically placed after regular columns
- The ressortgroupref system enables efficient handling of complex sorting and grouping operations
- Several fields are marked with pg_node_attr(query_jumble_ignore) to exclude them from query fingerprinting
- Essential for implementing SQL semantics around column ordering, naming, and result set construction
- Used in all major query types: SELECT, INSERT, UPDATE, and various utility commands
- The expr field can contain arbitrarily complex expressions, not just simple column references