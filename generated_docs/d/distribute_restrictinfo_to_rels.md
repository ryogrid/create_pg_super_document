# distribute_restrictinfo_to_rels

## Location
[src/backend/optimizer/plan/initsplan.c:2876-2960](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/optimizer/plan/initsplan.c#L2876-L2960)

## Overview
Distributes a completed RestrictInfo clause to the appropriate relation restriction lists or join clause lists based on which relations the clause references.

## Definition

```c
structure with
	 * original (this is necessary in case there are subselects in there...)
	 */
	clause = (Node *) make_opclause(opno,
									BOOLOID,	/* opresulttype */
									false,	/* opretset */
									copyObject(item1),
									copyObject(item2),
									InvalidOid,
									collation);
```
## Detailed Description
This function is the final step in the qualify distribution process for ordinary qualification clauses in PostgreSQL's query planner. It analyzes the  bitmapset of a RestrictInfo to determine where the clause should be attached:

1. **Single relation**: If the clause references only one relation, it's treated as a restriction (WHERE) clause and added to that relation's restriction list
2. **Multiple relations**: If the clause references multiple relations, it's treated as a join clause and distributed to all relevant relations' join lists

For join clauses, the function also performs additional optimization preparations by checking if the clause is suitable for hash joins and memoization during nested loop joins.

## Parameters / Member Variables
- : PlannerInfo structure containing planner state and global query information
- : The completed RestrictInfo clause to be distributed to appropriate relation lists

## Dependencies
- Functions called/Symbols referenced:
  - bms_is_empty (checks if bitmapset is empty)
  - [bms_get_singleton_member](../b/bms_get_singleton_member.md) (extracts single member from bitmapset)
  - [add_base_clause_to_rel](../a/add_base_clause_to_rel.md) (adds restriction clause to base relation)
  - [check_hashjoinable](../c/check_hashjoinable.md) (analyzes clause for hash join suitability)
  - [check_memoizable](../c/check_memoizable.md) (analyzes clause for memoization potential)
  - [add_join_clause_to_rels](../a/add_join_clause_to_rels.md) (distributes join clause to relevant relations)

- Called from (representative examples):
  - [distribute_qual_to_rels](distribute_qual_to_rels.md) (main qualification distribution function)
  - [process_implied_equality](../p/process_implied_equality.md) (equivalence class processing)
  - [generate_base_implied_equalities_const](../g/generate_base_implied_equalities_const.md) (equivalence class constant generation)
  - [reconsider_outer_join_clauses](../r/reconsider_outer_join_clauses.md) (outer join clause reconsideration)

## Notes and Other Information
- Final step of the qualification distribution pipeline after equivalence class processing
- Performs join optimization analysis (hash join, memoization) only for true join clauses
- Includes error handling for degenerate clauses that reference no relations
- Critical component of PostgreSQL's query planning infrastructure for organizing query conditions
- Works in conjunction with the equivalence class machinery for advanced optimization scenarios
- Part of the broader qualification distribution system that transforms SQL WHERE/JOIN conditions into the internal planner representation