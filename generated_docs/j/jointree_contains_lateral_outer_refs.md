# jointree_contains_lateral_outer_refs

## Location
[src/backend/optimizer/prep/prepjointree.c:2191-2265](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/optimizer/prep/prepjointree.c#L2191-L2265)

## Overview
This function checks for disallowed lateral references in a jointree's quals, enforcing scoping rules for LATERAL references in SQL queries.

## Definition
```c
static bool jointree_contains_lateral_outer_refs(PlannerInfo *root, Node *jtnode, bool restricted, Relids safe_upper_varnos)
```

## Detailed Description
The jointree_contains_lateral_outer_refs function recursively traverses a query's jointree to detect lateral references that violate SQL scoping rules. LATERAL references allow a table-valued function or subquery to reference columns from relations that appear earlier in the FROM clause, but there are strict rules about where such references are allowed.

The function operates in two modes based on the restricted parameter:
- **Unrestricted mode** (restricted=false): All level-1 Vars are allowed, but the function still traverses the tree since outer joins below may impose restrictions
- **Restricted mode** (restricted=true): Returns true if any qual contains level-1 Vars from relations not listed in safe_upper_varnos

The function handles three types of jointree nodes:
1. **RangeTblRef**: Base case - no lateral references possible in a simple table reference
2. **FromExpr**: Recursively checks child joins, then examines top-level quals for restricted lateral references  
3. **JoinExpr**: Special handling for outer joins which automatically enable restricted mode, then recursively checks both left and right arguments plus JOIN quals

A key rule enforced is that outer joins (LEFT, RIGHT, FULL) disallow any upper lateral references in or below them, as this would violate the semantic guarantees of outer joins.

## Parameters / Member Variables
- `root`: PlannerInfo structure containing query context and metadata
- `jtnode`: Node in the jointree being examined (RangeTblRef, FromExpr, or JoinExpr)
- `restricted`: Boolean indicating whether to enforce lateral reference restrictions
- `safe_upper_varnos`: Relids bitmap indicating which relations can be safely referenced when restricted=true

## Dependencies
- Functions called/Symbols referenced:
  - RangeTblRef
  - FromExpr  
  - JoinExpr
  - [bms_is_subset](../b/bms_is_subset.md)
  - [pull_varnos_of_level](../p/pull_varnos_of_level.md)
  - JOIN_INNER
  - nodeTag
  - [jointree_contains_lateral_outer_refs](jointree_contains_lateral_outer_refs.md) (recursive calls)
- Called from:
  - [is_simple_subquery](../i/is_simple_subquery.md)
  - [jointree_contains_lateral_outer_refs](jointree_contains_lateral_outer_refs.md) (recursive)

## Notes and Other Information
- Enforces SQL LATERAL scoping rules to prevent illegal cross-references
- Outer joins automatically trigger restricted mode to preserve their semantic guarantees  
- Uses pull_varnos_of_level to extract variable references at a specific query level
- The function is essential for maintaining correctness of LATERAL query optimizations
- Recursive traversal handles arbitrarily complex jointree structures
- Part of PostgreSQL's subquery pullup safety infrastructure
- Located in src/backend/optimizer/prep/prepjointree.c:2191-2265