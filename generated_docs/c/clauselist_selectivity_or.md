# clauselist_selectivity_or

## Location
src/backend/optimizer/path/clausesel.c: 359 - 426

## Overview
Computes the selectivity of an implicitly-ORed list of boolean expression clauses, implementing sophisticated OR logic estimation with extended statistics support and accounting for tuple set overlap.

## Definition
```c
static Selectivity clauselist_selectivity_or(PlannerInfo *root,
                                             List *clauses,
                                             int varRelid,
                                             JoinType jointype,
                                             SpecialJoinInfo *sjinfo,
                                             bool use_extended_stats)
```

## Detailed Description
This function estimates the selectivity of clauses combined with OR logic, which is significantly more complex than AND combinations due to the need to account for overlap between selected tuple sets. The function employs a two-phase approach:

1. **Extended Statistics Phase**: When `use_extended_stats` is true and clauses reference a single relation with extended statistics, applies cross-column correlation analysis via `statext_clauselist_selectivity` with OR logic enabled.

2. **Independence Assumption Phase**: For remaining clauses not covered by extended statistics, uses the standard OR probability formula: s1 + s2 - s1*s2, which accounts for the overlap of selected tuple sets while assuming independence.

Key characteristics:
- Returns 0.0 for empty clause lists (no rows selected)
- Supports both RestrictInfo structures and bare expression clauses
- Uses extended statistics when available for better correlation handling
- Applies conservative overlap correction for independent clauses
- Maintains bitmap tracking of already-estimated clauses to avoid double-counting

The function is essential for estimating selectivity of OR expressions in WHERE clauses, HAVING clauses, and join conditions.

## Parameters / Member Variables
- `root`: PlannerInfo structure containing query planning context and relation statistics
- `clauses`: List of boolean expression clauses to be ORed together (can be empty)
- `varRelid`: Variable relation ID for parameterized path estimation context
- `jointype`: Join type affecting selectivity calculation approach
- `sjinfo`: Special join information for complex join scenario handling
- `use_extended_stats`: Boolean flag controlling extended statistics usage for cross-column correlations

## Dependencies
- Functions called/Symbols referenced:
  - [find_single_rel_for_clauses](../f/find_single_rel_for_clauses.md)
  - [statext_clauselist_selectivity](../s/statext_clauselist_selectivity.md)
  - [bms_is_member](../b/bms_is_member.md)
  - [clause_selectivity_ext](clause_selectivity_ext.md)
  - RTE_RELATION
  - JoinType
  - [SpecialJoinInfo](../S/SpecialJoinInfo.md)
- Called from (representative examples):
  - [clause_selectivity_ext](clause_selectivity_ext.md)

## Notes and Other Information
This function implements the probabilistic OR formula s1 + s2 - s1*s2, which correctly accounts for tuple set overlap. The formula derives from set theory: |A ∪ B| = |A| + |B| - |A ∩ B|, where the intersection term prevents double-counting overlapping selections.

Key implementation details:
- The function is marked static, indicating internal use within clausesel.c
- Extended statistics integration allows capturing real correlations in OR expressions
- The comment "XXX is this too conservative?" suggests ongoing research into better OR selectivity models
- Bitmap-based clause tracking ensures each clause is estimated exactly once
- The algorithm gracefully handles mixed scenarios where some clauses benefit from extended statistics while others fall back to independence assumptions
- Critical for accurate cost estimation in queries with complex OR predicates, particularly those involving multiple columns from the same relation