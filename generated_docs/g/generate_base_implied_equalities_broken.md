# generate_base_implied_equalities_broken

## Location
[src/backend/optimizer/path/equivclass.c:1313-1375](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/optimizer/path/equivclass.c#L1313-L1375)

## Overview
A cleanup function that handles the restoration of RestrictInfo clauses back to the main restrictinfo datastructures when an EquivalenceClass becomes broken during base relation processing.

## Definition
```c
static void generate_base_implied_equalities_broken(PlannerInfo *root, EquivalenceClass *ec)
```

## Detailed Description
This function is called when an EquivalenceClass (EC) becomes broken during base relation processing and needs cleanup. The function implements a strategy to push zero- or one-relation source RestrictInfos from the broken EC back into the main restrictinfo datastructures. 

For ECs containing constants (ec_has_const), it adopts a simpler approach by throwing back all source RestrictInfos immediately, since such ECs cannot become broken later. Multi-relation clauses are deliberately left for later processing by generate_join_implied_equalities() to maintain continuity with cases where the EC becomes broken only after ascending join levels.

The function maintains the invariant that constant-containing ECs can be safely processed immediately without affecting future join planning phases.

## Parameters / Member Variables
- `root`: The PlannerInfo structure containing global planner state and context information
- `ec`: The EquivalenceClass that has become broken and needs cleanup processing

## Dependencies
- Functions called/Symbols referenced:
  - [bms_membership](../b/bms_membership.md)
  - [distribute_restrictinfo_to_rels](../d/distribute_restrictinfo_to_rels.md)
- Called from (representative examples):
  - [generate_base_implied_equalities](generate_base_implied_equalities.md) (src/backend/optimizer/path/equivclass.c:1066)

## Notes and Other Information
- This is a static helper function specifically for handling broken EquivalenceClass cleanup
- The function uses different strategies based on whether the EC contains constants (ec_has_const flag)
- Multi-relation clauses are intentionally deferred to join-level processing for consistency
- The BMS_MULTIPLE check determines if a RestrictInfo applies to multiple relations
- Part of PostgreSQL's query optimization equivalence class management system