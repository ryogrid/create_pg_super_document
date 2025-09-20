# GroupByOrdering

## Location
[src/include/nodes/pathnodes.h:1485-1491](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/include/nodes/pathnodes.h#L1485-L1491)

## Overview
GroupByOrdering represents an ordered arrangement of GROUP BY clauses along with their corresponding pathkeys, used by PostgreSQL's query planner to optimize grouping operations.

## Definition

```c
typedef struct GroupByOrdering
{
	NodeTag		type;

	List	   *pathkeys;
	List	   *clauses;
} GroupByOrdering;
```
## Detailed Description
GroupByOrdering is a data structure that contains an ordered list of GROUP BY clauses and their corresponding pathkeys. This structure is essential for the query planner when determining optimal grouping strategies. The pathkeys represent the sort order that would be useful for grouping operations, while the clauses contain the actual GROUP BY expressions.

The structure enforces a specific relationship between its two main components: the elements in the 'clauses' list must maintain the same order as the head of the 'pathkeys' list. Additionally, the tleSortGroupRef of each clause should match the ec_sortref of the corresponding pathkey's equivalence class. When there are redundant clauses with identical tleSortGroupRef values, they must be grouped together to maintain consistency.

## Parameters / Member Variables
- `type`: NodeTag identifier for the structure type
- `*pathkeys`: List of pathkeys representing the sort order useful for grouping operations
- `*clauses`: List of GROUP BY clauses ordered to correspond with the pathkeys
## Dependencies
- Functions called/Symbols referenced:
  - NodeTag (for type identification)
  - [List](../L/List.md) (PostgreSQL's list structure)

- Called from (representative examples):
  - [get_useful_group_keys_orderings](../g/get_useful_group_keys_orderings.md) (src/backend/optimizer/path/pathkeys.c:469-530)
  - [add_paths_to_grouping_rel](../a/add_paths_to_grouping_rel.md) (src/backend/optimizer/plan/planner.c:7080, 7161)
  - [create_partial_grouping_paths](../c/create_partial_grouping_paths.md) (src/backend/optimizer/plan/planner.c:7412, 7468)

## Notes and Other Information
- The structure maintains a strict ordering constraint between pathkeys and clauses
- Redundant clauses with the same tleSortGroupRef must be grouped together
- This structure is primarily used in query planning phases for optimizing GROUP BY operations
- The tleSortGroupRef values in clauses must match ec_sortref values in pathkey equivalence classes