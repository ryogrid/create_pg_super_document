# BuildParameterizedTidPaths

## Location
src/backend/optimizer/path/tidpath.c: 415 - 469

## Overview
A static function that creates parameterized TID scan paths for join clauses involving the specified relation, enabling efficient tuple access through external parameter values.

## Definition
```c
static void BuildParameterizedTidPaths(PlannerInfo *root, RelOptInfo *rel, List *clauses)
```

## Detailed Description
BuildParameterizedTidPaths generates parameterized scan paths that can use TID values provided by outer relations in join operations. This optimization allows direct tuple access when join conditions specify exact CTID values, bypassing traditional index or sequential scan methods.

The function implements comprehensive validation and path creation:

**Validation Process:**
- Rejects pseudoconstant clauses that cannot contain variables
- Enforces security restrictions using restriction_is_securely_promotable  
- Verifies clauses are TidEqual types (no ScalarArrayOpExpr or CurrentOfExpr support)
- Ensures join clauses can be legally moved to the target relation

**Path Creation:**
- Computes required outer relations by combining restriction requirements with lateral dependencies
- Excludes the target relation itself from the required outer set
- Creates TID scan paths with appropriate parameterization
- Adds completed paths to the relation's path list

The function intentionally focuses on simple TidEqual clauses, as more complex expressions are unlikely to provide sufficient benefit to justify the additional complexity.

## Parameters / Member Variables
- `root`: PlannerInfo pointer containing global optimizer state and configuration
- `rel`: RelOptInfo pointer representing the target relation for parameterized path creation
- `clauses`: List of RestrictInfo structures representing potential join clauses to evaluate

## Dependencies
- Functions called/Symbols referenced:
  - restriction_is_securely_promotable
  - IsTidEqualClause
  - join_clause_is_movable_to
  - bms_union
  - bms_del_member
  - add_path
  - create_tidscan_path
- Called from (representative examples):
  - create_tidscan_paths

## Notes and Other Information
- Static function accessible only within tidpath.c
- Focuses exclusively on TidEqual clauses for simplicity and effectiveness
- Does not attempt to combine multiple clauses referencing the same outer relations
- Part of PostgreSQL's parameterized path infrastructure for join optimization
- Security-aware through promotion validation checks
- Creates paths that can be efficiently executed when outer relation values are available
- Essential for nested loop join optimizations involving direct tuple access
- Each clause generates a separate parameterized path rather than combining compatible clauses