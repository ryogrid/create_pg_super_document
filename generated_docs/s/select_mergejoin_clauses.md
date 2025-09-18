# select_mergejoin_clauses

## Location
src/backend/optimizer/path/joinpath.c: 2347 - 2433

## Overview
Selects and validates mergejoin clauses that are usable for a particular join operation, returning a list of suitable RestrictInfo nodes and determining if mergejoin is allowed for the given join type.

## Definition
```c
static List *select_mergejoin_clauses(PlannerInfo *root,
                                     RelOptInfo *joinrel,
                                     RelOptInfo *outerrel,
                                     RelOptInfo *innerrel,
                                     List *restrictlist,
                                     JoinType jointype,
                                     bool *mergejoin_allowed)
```

## Detailed Description
This function examines each restriction clause in the join's restrictinfo list to identify clauses suitable for merge join operations. It performs comprehensive validation including checking if clauses are mergejoinable, involve the correct relations, and meet equivalence class requirements.

The function handles different join types with varying restrictions. For outer joins, it only considers the join's own clauses, while inner joins can use pushed-down clauses. Special handling is applied for right, right-anti, and full joins where the presence of non-mergejoinable clauses makes mergejoin unsafe.

The function also ensures that each mergejoin clause can be associated with non-redundant equivalence classes, which is required for proper pathkey handling in the planner. It marks selected RestrictInfo nodes to indicate which side is considered as outer for the duration of the current planning phase.

## Parameters / Member Variables
- `root`: PlannerInfo structure containing global planner state and context
- `joinrel`: The target join relation for which clauses are being selected
- `outerrel`: The outer relation in the join operation
- `innerrel`: The inner relation in the join operation
- `restrictlist`: List of restriction clauses to examine for mergejoin suitability
- `jointype`: The type of join operation (INNER, RIGHT, FULL, etc.)
- `mergejoin_allowed`: Output parameter indicating whether mergejoin is safe for this join type

## Dependencies
- Functions called/Symbols referenced:
  - JoinType (enum type)
  - IS_OUTER_JOIN
  - RINFO_IS_PUSHED_DOWN
  - clause_sides_match_join
  - update_mergeclause_eclasses
  - EC_MUST_BE_REDUNDANT
  - JOIN_RIGHT, JOIN_RIGHT_ANTI, JOIN_FULL (enum values)
- Called from (representative examples):
  - add_paths_to_joinrel

## Notes and Other Information
- This is a static function, meaning it's only accessible within the joinpath.c compilation unit
- The function sets mergejoin_allowed to false for RIGHT, RIGHT_ANTI, and FULL joins when non-mergejoinable clauses are present, as the executor cannot handle such combinations
- Pushed-down clauses are treated differently for outer vs inner joins - they become otherquals rather than joinquals for outer joins
- The function supports constant clauses in right/right-anti/full joins to enable constructs like 'FULL JOIN ON FALSE'
- Equivalence class validation ensures that merge clauses can be properly associated with canonical pathkey lists
- The RestrictInfo markings applied by this function are transient and only valid for the current add_paths_to_joinrel() call
- The function can return an empty list while still allowing mergejoin (for clauseless mergejoins) - the mergejoin_allowed flag provides the definitive safety indication