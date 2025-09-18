# make_limit

## Location
src/backend/optimizer/plan/createplan.c: 6961 - 6988

## Overview
Creates a Limit plan node that restricts the number of tuples returned by its child plan, implementing SQL LIMIT and OFFSET clauses.

## Definition


## Detailed Description
The  function constructs a Limit plan node that implements SQL LIMIT and OFFSET functionality by controlling the number of tuples passed from its child plan to parent nodes. The node can skip a specified number of initial tuples (OFFSET) and then return up to a specified maximum number of subsequent tuples (LIMIT). Additionally, it supports uniqueness constraints through optional column specifications for duplicate elimination.

The Limit node is essential for implementing query result pagination and limiting resource usage in large result sets. It can also enforce DISTINCT operations when uniqueness parameters are provided, making it a versatile node for both row limiting and duplicate elimination scenarios.

## Parameters / Member Variables
- : The child plan node providing input tuples to limit
- : Expression specifying how many initial tuples to skip (OFFSET clause)
- : Expression specifying the maximum number of tuples to return (LIMIT clause)
- : Options controlling limit behavior and optimization strategies
- : Number of columns to consider for uniqueness checking (0 if no uniqueness required)
- : Array of column indices to use for uniqueness comparison
- : Array of equality operators for uniqueness comparison
- : Array of collations to use when comparing columns for uniqueness

## Dependencies
- Functions called/Symbols referenced:
  - makeNode (creates a new Limit node)
  - Limit (the plan node type being created)
  - [LimitOption](../L/LimitOption.md) (enumeration for limit behavior options)
- Called from (representative examples):
  - [create_limit_plan](../c/create_limit_plan.md)
  - [create_minmaxagg_plan](../c/create_minmaxagg_plan.md)
  - DEFAULT_CURSOR_TUPLE_FRACTION

## Notes and Other Information
- Unlike most other make_* functions, this one is not static and can be called from other files
- The target list is copied directly from the child plan as limiting doesn't change tuple structure
- Sets  to NIL since row filtering is handled by child nodes
- The uniqueness parameters (uniqNumCols, uniqColIdx, etc.) allow the Limit node to also perform DISTINCT operations
- Both limitOffset and limitCount can be NULL, allowing for OFFSET-only or LIMIT-only operations
- The limitOption parameter provides hints for optimization, such as whether the limit count is constant
- This node is crucial for implementing efficient pagination in applications and preventing resource exhaustion from unbounded queries