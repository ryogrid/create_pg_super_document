# have_relevant_eclass_joinclause

## Location
[src/backend/optimizer/path/equivclass.c:3087-3162](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/optimizer/path/equivclass.c#L3087-L3162)

## Overview
Detects whether there exists an EquivalenceClass that could produce a join clause involving two given relations.

## Definition

```c
bool
have_relevant_eclass_joinclause(PlannerInfo *root,
								RelOptInfo *rel1, RelOptInfo *rel2)
```
## Detailed Description
This function serves as a lightweight heuristic to determine whether two relations could potentially be joined via an equivalence class-derived join clause. It is essentially a simplified version of generate_join_implied_equalities() designed for quick decision-making during join planning. The function examines equivalence classes that mention both relations and checks if they have multiple members, which would indicate the potential for join clause generation.

The function is designed to be optimistic and may occasionally return false positives (saying "yes" when no actual join clause could be generated), but this is acceptable as it only influences join pathway exploration priority. It deliberately avoids checking complex details like cross-type operator availability or equivalence class integrity (ec_broken), as these would be expensive to verify for a heuristic function.

## Parameters / Member Variables
- : PlannerInfo structure containing global planning state and equivalence class information
- : First RelOptInfo to check for potential join clauses
- : Second RelOptInfo to check for potential join clauses

## Dependencies
- Functions called/Symbols referenced:
  - [get_common_eclass_indexes](../g/get_common_eclass_indexes.md)
  - [bms_next_member](../b/bms_next_member.md)
  - [list_nth](../l/list_nth.md)
  - [bms_overlap](../b/bms_overlap.md)
  - [list_length](../l/list_length.md)
- Called from (representative examples):
  - [have_relevant_joinclause](have_relevant_joinclause.md)

## Notes and Other Information
- Optimistic heuristic that may produce false positives but avoids false negatives
- Does not verify cross-type operator availability for performance reasons
- Ignores ec_broken status as a possibly-overoptimistic heuristic
- Considers const equivalence classes as potentially worth joining
- Only examines equivalence classes mentioning both input relations
- Part of PostgreSQL's join ordering optimization framework
- Located in src/backend/optimizer/path/equivclass.c:3087-3162