# find_nonnullable_rels_walker

## Location
src/backend/optimizer/util/clauses.c: 1462 - 1706

## Overview
The `find_nonnullable_rels_walker` function recursively traverses expression trees to identify which base relations are forced to be nonnullable by the given expression, supporting PostgreSQL's outer join optimization logic.

## Definition
```c
static Relids find_nonnullable_rels_walker(Node *node, bool top_level)
```

## Detailed Description
This function implements the core tree-walking logic for determining which relations cannot be all-NULL when an expression evaluates successfully. It performs detailed analysis of different node types to understand their strictness properties and how NULL values propagate through the expression tree.

The function handles two distinct contexts based on the `top_level` parameter:
- **Top level (true)**: Analyzing clauses where FALSE-or-NULL results are equivalent for determining nonnullable relations
- **Below top level (false)**: Analyzing within strict functions where NULL inputs must produce NULL outputs

Key analysis patterns include:

- **Variables**: Relations referenced by variables at the current query level are added to the result
- **Lists**: Union semantics - any arm that forces relations nonnullable contributes to the result
- **Strict functions/operators**: If function is strict, all argument relations become nonnullable
- **Boolean expressions**: Complex logic for AND/OR handling depends on top_level context
- **Type coercion nodes**: Transparent - pass through to the underlying expression
- **NULL tests**: IS NOT NULL tests make relations nonnullable at top level
- **SubPlans**: Special handling for ANY_SUBLINK and ROWCOMPARE_SUBLINK based on context
- **PlaceHolderVars**: Inherit nonnullability from contained expression, with special singleton handling

## Parameters / Member Variables
- `node`: A Node pointer representing the current expression node to analyze
- `top_level`: Boolean indicating whether this is top-level analysis (TRUE) or within a strict function context (FALSE)

## Dependencies
- Functions called/Symbols referenced:
  - [bms_make_singleton](../b/bms_make_singleton.md)
  - [bms_join](../b/bms_join.md)
  - [bms_int_members](../b/bms_int_members.md)
  - bms_is_empty
  - [bms_add_members](../b/bms_add_members.md)
  - [bms_membership](../b/bms_membership.md)
  - [func_strict](func_strict.md)
  - set_opfuncid
  - [is_strict_saop](../i/is_strict_saop.md)
- Called from (representative examples):
  - [find_nonnullable_rels](find_nonnullable_rels.md)
  - max_parallel_hazard_context (self-recursively)

## Notes and Other Information
- Returns a Relids bitmapset containing relation OIDs that must be nonnullable
- Static function used internally within clauses.c
- Implements sophisticated logic for Boolean expression handling (AND vs OR semantics)
- Special optimization for early termination when intersection becomes empty in OR expressions
- Handles complex expression types including subqueries, type coercions, and placeholder variables
- Critical component of PostgreSQL's outer join elimination and optimization infrastructure
- Uses conservative analysis - safe to miss some nonnullable relations but must never incorrectly identify them
- Located in src/backend/optimizer/util/clauses.c:1462-1706