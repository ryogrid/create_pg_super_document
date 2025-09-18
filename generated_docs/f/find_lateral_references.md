# find_lateral_references

## Location
[src/backend/optimizer/plan/initsplan.c:358-405](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/optimizer/plan/initsplan.c#L358-L405)

## Overview
Processes LATERAL subqueries to extract references to Vars and PlaceHolderVars and ensures those values will be available for subquery evaluation.

## Definition
```c
void find_lateral_references(PlannerInfo *root)
```

## Detailed Description
This function handles the setup of LATERAL reference dependencies in unflattened LATERAL subqueries. It ensures that variables referenced by LATERAL subqueries from the outer query levels are properly marked as needed and will propagate up to the appropriate join levels.

The function works by:
1. **Early exit optimization**: Returns immediately if the query contains no LATERAL RTEs (root->hasLateralRTEs is false)
2. **Baserel iteration**: Examines all baserels in the simple_rel_array
3. **Filtering**: Only processes RELOPT_BASEREL relations, ignoring appendrel otherrels
4. **Delegation**: Calls extract_lateral_references() for each qualifying base relation

The design choice to process only parent baserels (not their appendrel children) is deliberate - the parent's RTE contains all necessary lateral references since pulled-up members are copies of the original RTE's subquery parts.

## Parameters / Member Variables
- `root`: PlannerInfo structure containing query planning context, including hasLateralRTEs flag and simple_rel_array

## Dependencies
- Functions called/Symbols referenced:
  - [extract_lateral_references](../e/extract_lateral_references.md)
  - RELOPT_BASEREL (constant)
- Data structures used:
  - RelOptInfo
  - Index (for RTI iteration)
- Called from (representative examples):
  - [query_planner](../q/query_planner.md)

## Notes and Other Information
- Must run before deconstruct_jointree since it might create PlaceHolderInfos
- Only handles unflattened LATERAL subqueries; flattened ones are handled elsewhere
- When LATERAL subqueries are flattened, their references become plain Vars but may need PlaceHolderVar wrapping for NULL-forcing outer joins
- Ignores appendrel otherrels in favor of processing their parent baserels for join planning efficiency
- The hasLateralRTEs flag provides an important optimization to avoid unnecessary work
- Located in src/backend/optimizer/plan/initsplan.c at lines 358-405