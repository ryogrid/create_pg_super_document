# pathkeys_contained_in

## Location
[src/backend/optimizer/path/pathkeys.c:341-367](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/optimizer/path/pathkeys.c#L341-L367)

## Overview
Determines if the second pathkey list provides at least as good sorting as the first pathkey list, serving as a specialized case of pathkey comparison.

## Definition
```c
bool pathkeys_contained_in(List *keys1, List *keys2)
```

## Detailed Description
The pathkeys_contained_in function is a convenience wrapper around compare_pathkeys that answers a specific question: "Does keys2 provide at least as good sorting as keys1?" This function is commonly used in query optimization to determine if an existing sort order can satisfy a required sort order without additional sorting operations.

The function delegates to compare_pathkeys and returns true if the comparison result is either PATHKEYS_EQUAL (both lists are identical) or PATHKEYS_BETTER2 (keys2 is a superset/extension of keys1). In all other cases (PATHKEYS_DIFFERENT or PATHKEYS_BETTER1), it returns false, indicating that keys2 cannot satisfy the ordering requirements of keys1.

## Parameters / Member Variables
- `keys1`: The required pathkey list (what we need)
- `keys2`: The available pathkey list (what we have)

## Dependencies
- Functions called/Symbols referenced:
  - [compare_pathkeys](../c/compare_pathkeys.md) (performs the actual comparison)
  - PATHKEYS_EQUAL (comparison result constant)  
  - PATHKEYS_BETTER2 (comparison result constant)
- Called from (representative examples):
  - [generate_orderedappend_paths](../g/generate_orderedappend_paths.md)
  - [cost_append](../c/cost_append.md)
  - [try_mergejoin_path](../t/try_mergejoin_path.md)
  - [get_useful_group_keys_orderings](../g/get_useful_group_keys_orderings.md)
  - [get_cheapest_path_for_pathkeys](../g/get_cheapest_path_for_pathkeys.md)
  - [create_append_plan](../c/create_append_plan.md)

## Notes and Other Information
This function is frequently used in path generation and costing to determine if existing sort orders can be leveraged to avoid additional sort operations. It plays a crucial role in merge join optimization, append path generation, and general pathkey compatibility checking throughout the query planner.

## Simplified Source

```c
bool
pathkeys_contained_in(List *keys1, List *keys2)
{
    // Check if keys2 provides at least as good sorting as keys1
    switch (compare_pathkeys(keys1, keys2)) {
        case PATHKEYS_EQUAL:     // Same ordering
        case PATHKEYS_BETTER2:   // keys2 is superset of keys1
            return true;
        default:
            break;
    }
    return false;
}
```