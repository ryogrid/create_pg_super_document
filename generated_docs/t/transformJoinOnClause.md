# transformJoinOnClause

## Location
src/backend/parser/parse_clause.c: 367 - 396

## Overview
Transforms the qualification conditions for JOIN/ON clauses by setting up the appropriate namespace context and processing the boolean expressions.

## Definition


## Detailed Description
The `transformJoinOnClause` function is a static helper function responsible for transforming the ON clause conditions in JOIN expressions. It performs namespace management to ensure that the ON clause expressions can only see the appropriate set of relations and columns.

The function operates through these key steps:
1. **Namespace preparation**: Sets up the correct namespace containing only the two JOIN subtrees plus any outer references from higher pstate levels
2. **LATERAL state management**: Marks all namespace items as visible regardless of LATERAL state using `setNamespaceLateralState`
3. **Namespace swapping**: Temporarily replaces the current pstate namespace with the JOIN-specific namespace
4. **Expression transformation**: Delegates to `transformWhereClause` to transform the actual qualification expressions with JOIN/ON context
5. **Namespace restoration**: Restores the original pstate namespace after transformation

This careful namespace management ensures that JOIN ON clauses can only reference columns from the joined relations and any outer query levels, preventing inappropriate column references and maintaining SQL semantic correctness.

## Parameters / Member Variables
- `pstate`: The current parse state containing parsing context and namespace information
- `j`: The JoinExpr structure containing the JOIN information and qualification conditions
- `namespace`: List of ParseNamespaceItem structures representing the namespace visible to the JOIN ON clause

## Dependencies
- Functions called/Symbols referenced:
  - JoinExpr
  - setNamespaceLateralState
  - transformWhereClause
  - EXPR_KIND_JOIN_ON
  - ParseNamespaceItem
- Called from (representative examples):
  - transformFromClauseItem

## Notes and Other Information
- This is a static (internal) function within parse_clause.c, not exposed in the public API
- Ensures proper namespace isolation for JOIN ON clauses to maintain SQL semantic rules
- No refname conflict checking needed as it's already handled by transformFromClauseItem()
- All namespace items are made visible regardless of LATERAL state during ON clause processing
- Uses the same underlying transformation logic as WHERE clauses via transformWhereClause()
- Critical for maintaining the correct scope and visibility rules in complex JOIN expressions
- The temporary namespace includes exactly the two JOIN subtrees plus outer references