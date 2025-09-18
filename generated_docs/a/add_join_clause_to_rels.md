# add_join_clause_to_rels

## Location
src/backend/optimizer/util/joininfo.c: 98 - 160

## Overview
Adds a join restriction clause to the joininfo list of each relation that participates in the join, enabling the query optimizer to track which relations can be joined together.

## Definition
```c
void add_join_clause_to_rels(PlannerInfo *root, RestrictInfo *restrictinfo, Relids join_relids)
```

## Detailed Description
This function distributes a join restriction clause to all participating base relations by adding it to their joininfo lists. The same RestrictInfo node is shared across all lists to enable caching of information about the restriction clause, though care must be taken that cached information is context-independent.

The function performs several optimizations:
1. Skips adding clauses that are always true (trivial conditions)
2. Converts always-false clauses to constant-FALSE while preserving the rinfo_serial to maintain consistency for identical conditions
3. Only adds clauses to base relations, skipping join relations

The preservation of rinfo_serial numbers is critical for ensuring that RestrictInfos representing the "same" qualifier condition receive identical serial numbers, which is essential for proper handling in functions like deconstruct_distribute_oj_quals.

## Parameters / Member Variables
- `root`: PlannerInfo structure containing global planning information
- `restrictinfo`: RestrictInfo node describing the join clause to be distributed
- `join_relids`: Bitmap set of relation IDs participating in the join clause

## Dependencies
- Functions called/Symbols referenced:
  - restriction_is_always_true
  - restriction_is_always_false  
  - make_restrictinfo
  - makeBoolConst
  - bms_next_member
  - find_base_rel_ignore_join
- Called from (representative examples):
  - distribute_restrictinfo_to_rels

## Notes and Other Information
- The same RestrictInfo node is shared across multiple joininfo lists for efficiency
- Serial number preservation ensures consistency in restriction clause identification
- Only base relations receive the join clauses, not derived join relations
- Always-false conditions are converted to constant FALSE expressions for optimization
- Located in src/backend/optimizer/util/joininfo.c:98-160