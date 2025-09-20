# join_is_legal

## Location
[src/backend/optimizer/path/joinrels.c:350-669](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/optimizer/path/joinrels.c#L350-L669)

## Overview
Determines whether a proposed join between two relations is legal given the query's join order constraints and special join requirements, and identifies the appropriate join type and parameters.

## Definition

```c
static bool
join_is_legal(PlannerInfo *root, RelOptInfo *rel1, RelOptInfo *rel2,
			  Relids joinrelids,
			  SpecialJoinInfo **sjinfo_p, bool *reversed_p)
```
## Detailed Description
The  function is a critical component of PostgreSQL's join planning that validates whether two relations can be legally joined according to the query's constraints. It handles complex join order restrictions arising from:

1. **Special joins** (LEFT, RIGHT, FULL, SEMI, ANTI) with their specific ordering requirements
2. **LATERAL references** that impose nestloop join requirements
3. **Join order restrictions** that prevent certain join combinations

The function performs comprehensive analysis including:
- Scanning the join info list for relevant SpecialJoinInfo nodes
- Checking for proper left/right hand side containment in special joins
- Handling semijoin unique-ification scenarios for optimization opportunities
- Validating LATERAL reference constraints and nestloop feasibility
- Ensuring joins can be parameterized correctly without creating impossible plans

## Parameters / Member Variables
- : PlannerInfo structure containing query planning context
- : First relation to be joined
- : Second relation to be joined  
- : Union of relids from both relations (pre-computed for efficiency)
- : Output parameter set to the matching SpecialJoinInfo node (NULL for inner joins)
- : Output parameter indicating if relations need to be swapped to match the SpecialJoinInfo

## Dependencies
- Functions called/Symbols referenced:
  - [bms_overlap](../b/bms_overlap.md)
  - [bms_is_subset](../b/bms_is_subset.md)
  - [bms_equal](../b/bms_equal.md)
  - [create_unique_path](../c/create_unique_path.md)
  - [have_dangerous_phv](../h/have_dangerous_phv.md)
  - min_join_parameterization
  - [bms_copy](../b/bms_copy.md)
  - [bms_add_members](../b/bms_add_members.md)
- Called from (representative examples):
  - [make_join_rel](../m/make_join_rel.md)
  - [has_legal_joinclause](../h/has_legal_joinclause.md)

## Notes and Other Information
- Returns false if the join violates any special join constraints or LATERAL reference requirements
- Handles complex cases like semijoin unique-ification where RHS relations can be made unique to enable more flexible join ordering
- Includes sophisticated logic for associating joins into special join RHS when previous constraint violations were deemed acceptable
- LATERAL reference handling ensures nestloop implementation feasibility and prevents dangerous parameterized hash variable scenarios
- The function's validation is essential for preventing the generation of invalid execution plans that could produce incorrect query results
- Static function scope restricts usage to within the same source file