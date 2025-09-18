# WithCheckOption

## Location
[src/include/nodes/parsenodes.h:1368-1376](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/include/nodes/parsenodes.h#L1368-L1376)

## Overview
WithCheckOption represents a WITH CHECK OPTION constraint used in views and Row Level Security (RLS) policies to enforce data validation during INSERT and UPDATE operations.

## Definition


## Detailed Description
WithCheckOption implements constraint checking for WITH CHECK OPTION clauses in views and Row Level Security policies. When a view is defined with WITH CHECK OPTION (either LOCAL or CASCADED), or when RLS policies are active, this structure ensures that INSERT and UPDATE operations produce rows that satisfy the specified constraints.

The structure supports different kinds of check options through the WCOKind enumeration, including view-based checks and RLS policy checks. For view constraints, the cascaded flag determines whether the check applies only to the current view (LOCAL) or to all underlying views in the hierarchy (CASCADED).

The qual field contains the constraint expression that must evaluate to true for the operation to succeed. During execution, if the constraint is violated, the operation fails with an appropriate error message. This mechanism is crucial for maintaining data integrity in complex view hierarchies and enforcing security policies.

## Parameters / Member Variables
- : NodeTag identifying this as a WithCheckOption node
- : Type of WITH CHECK OPTION (view-based or RLS policy-based)
- : Name of the relation that specified this WITH CHECK OPTION
- : Name of the RLS policy being checked (NULL for view-based WCOs)
- : Node containing the constraint qualification expression to evaluate
- : Whether this is a cascaded WITH CHECK OPTION for views (applies to view hierarchy)

## Dependencies
- Functions called/Symbols referenced:
  - NodeTag
  - [WCOKind](WCOKind.md)
  - [Node](../N/Node.md)
- Called from (representative examples):
  - [ExecWithCheckOptions](../E/ExecWithCheckOptions.md)
  - [ExecInitModifyTable](../E/ExecInitModifyTable.md)
  - [ExecInitMerge](../E/ExecInitMerge.md)
  - [rewriteTargetView](../r/rewriteTargetView.md)
  - [subquery_planner](../s/subquery_planner.md)
  - Row Level Security functions

## Notes and Other Information
- Essential for enforcing WITH CHECK OPTION constraints in updatable views
- Integrates with Row Level Security to enforce policy-based access control
- The cascaded flag implements SQL standard LOCAL vs CASCADED semantics
- Constraint violations result in runtime errors during INSERT/UPDATE operations
- Multiple WithCheckOption structures may exist for complex view hierarchies
- Used by the rewriter to transform view operations into base table operations
- Critical for maintaining data integrity and security in PostgreSQL's view system
- The qual expression is evaluated at execution time for each affected row