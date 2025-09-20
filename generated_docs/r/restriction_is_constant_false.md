# restriction_is_constant_false

## Location
[src/backend/optimizer/path/joinrels.c:1425-1478](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/optimizer/path/joinrels.c#L1425-L1478)

## Overview
Analyzes a restriction list to determine if it contains constant FALSE conditions, helping the optimizer avoid unnecessary computation in outer join scenarios where no matches are possible.

## Definition

```c
static bool
restriction_is_constant_false(List *restrictlist,
							  RelOptInfo *joinrel,
							  bool only_pushed_down)
```
## Detailed Description
This function examines a list of restriction clauses to identify cases where the restrictions are provably constant FALSE. This detection is crucial for optimization in outer join scenarios where the presence of FALSE conditions would mean that no outer row can find a match, making cartesian product computation wasteful.

While the optimizer's eval_const_expressions typically removes anything ANDed with FALSE constants, outer join situations can be more complex. The function handles cases where FALSE constants remain in the restriction list alongside other pushed-down qualifications from higher join levels.

The function iterates through each RestrictInfo in the list and performs the following checks:
1. If only_pushed_down is true, it considers only restrictions that are pushed down from the perspective of the joinrel
2. For each qualifying restriction, it examines if the clause is a Const node
3. It treats both explicit FALSE constants and NULL constants as indicating emptiness
4. Returns true if any FALSE/NULL constant is found, false otherwise

This optimization prevents the generation of expensive cartesian products when the result would inevitably be empty.

## Parameters / Member Variables
- : List of RestrictInfo nodes representing the restriction clauses to examine
- : RelOptInfo for the join relation, used to determine which restrictions are pushed down
- : Boolean flag indicating whether to consider only pushed-down restrictions

## Dependencies
- Functions called/Symbols referenced:
  -  - Macro to check if a RestrictInfo is pushed down to a specific relation set

- Called from (representative examples):
  -  - Uses this check to identify empty join results and avoid path generation

## Notes and Other Information
- The function is static and only used within joinrels.c
- Treats both FALSE and NULL constants as indicators of empty results
- Designed to optimize outer join scenarios by detecting impossible matches early
- Can operate in two modes: checking all restrictions or only pushed-down ones
- Part of PostgreSQL's optimization strategy to avoid unnecessary computation
- Returns true if constant FALSE detected (indicating empty result), false otherwise
- Located in src/backend/optimizer/path/joinrels.c:1425-1478