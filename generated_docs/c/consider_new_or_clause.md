# consider_new_or_clause

## Location
src/backend/optimizer/util/orclauses.c: 254 - 345

## Overview
Evaluates whether an extracted restriction OR clause is worth using and, if beneficial, adds it to the relation's restriction list while adjusting the original join clause's selectivity.

## Definition
```c
static void consider_new_or_clause(PlannerInfo *root, RelOptInfo *rel,
                                   Expr *orclause, RestrictInfo *join_or_rinfo)
```

## Detailed Description
This function performs a cost-benefit analysis on a successfully extracted restriction OR clause to determine if it should be added to the query optimization process. The function creates a RestrictInfo for the extracted clause, estimates its selectivity, and applies a threshold test to ensure the clause provides meaningful filtering.

The key decision criterion is selectivity: only clauses that reject a significant fraction of rows (selectivity ≤ 0.9) are considered worthwhile. This threshold prevents the addition of clauses that would cause duplicate computation without substantial benefit.

When a clause is deemed beneficial, the function:
1. Adds it to the relation's base restriction list
2. Updates the security level tracking
3. Compensates for redundancy by adjusting the original join clause's cached selectivity

The selectivity adjustment is described as a "MAJOR HACK" that prevents underestimation of join sizes due to the redundant restriction clauses. This compensation maintains cost estimation accuracy by ensuring the join relation size estimate remains approximately the same as it would have been without the extraction process.

## Parameters / Member Variables
- `root`: PlannerInfo containing the query planning context
- `rel`: RelOptInfo representing the relation to which the clause might be added
- `orclause`: The extracted OR clause expression to be evaluated
- `join_or_rinfo`: The original join OR clause RestrictInfo that needs selectivity adjustment

## Dependencies
- Functions called/Symbols referenced:
  - make_restrictinfo
  - clause_selectivity
  - JOIN_INNER (constant)
  - SpecialJoinInfo (struct)
  - init_dummy_sjinfo
  - bms_difference
- Called from (representative examples):
  - extract_restriction_or_clauses

## Notes and Other Information
- This is a static function only used within the orclauses.c module
- Uses an arbitrary selectivity threshold of 0.9 to determine clause worthiness
- The selectivity adjustment hack depends on cached selectivity values remaining stable
- Only adjusts norm_selec (JOIN_INNER semantics), not outer_selec, due to complexity with outer joins
- Updates both the restriction clause list and minimum security level tracking
- The selectivity compensation mechanism may not work correctly with nonlinear join size computations
- Creates a dummy SpecialJoinInfo for inner join selectivity calculations
- Ensures adjusted selectivity values remain within valid bounds (≤ 1.0)
- The function represents a balance between optimization opportunity and computational cost