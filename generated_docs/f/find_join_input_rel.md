# find_join_input_rel

## Location
[src/backend/utils/adt/selfuncs.c:6494-6525](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/selfuncs.c#L6494-L6525)

## Overview
Looks up the input relation for a join operation by finding the corresponding RelOptInfo structure for the given set of relation IDs.

## Definition
```c
static RelOptInfo *find_join_input_rel(PlannerInfo *root, Relids relids)
```

## Detailed Description
This function serves as a helper to locate RelOptInfo structures for join input relations during query planning. It handles both single relations and join relations by first checking if the relids set contains only one member (indicating a base relation) or multiple members (indicating a join relation). For single relations, it calls find_base_rel, while for join relations, it calls find_join_rel. The function assumes that the RelOptInfo structure for the requested relation has already been constructed during earlier phases of query planning. If no matching RelOptInfo is found, it raises an error since this indicates an internal inconsistency in the planner.

## Parameters / Member Variables
- `root`: PlannerInfo structure containing the query planning context
- `relids`: Relids (bitmap set) representing the relation IDs to look up

## Dependencies
- Functions called/Symbols referenced:
  - bms_is_empty
  - [bms_get_singleton_member](../b/bms_get_singleton_member.md)
  - [find_base_rel](find_base_rel.md)
  - [find_join_rel](find_join_rel.md)
- Called from (representative examples):
  - [eqjoinsel](../e/eqjoinsel.md)

## Notes and Other Information
This function is primarily used in selectivity estimation functions where the planner needs to access relation information for join operations. The function uses bitmap set operations to efficiently determine whether it's dealing with a single relation or a join of multiple relations. The error condition should never occur in normal operation, as it indicates that the planner is trying to reference a relation that hasn't been properly initialized, which would suggest a bug in the planning process. The function is static, meaning it's only used within the selfuncs.c module for internal selectivity calculations.