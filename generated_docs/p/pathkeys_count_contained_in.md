# pathkeys_count_contained_in

## Location
[src/backend/optimizer/path/pathkeys.c:556-617](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/optimizer/path/pathkeys.c#L556-L617)

## Overview
Extended version of pathkeys_contained_in that also determines the length of the longest common prefix between two pathkey lists, providing both containment information and prefix overlap count.

## Definition
```c
bool pathkeys_count_contained_in(List *keys1, List *keys2, int *n_common)
```

## Detailed Description
This function performs the same containment check as pathkeys_contained_in but additionally calculates and returns the number of common pathkeys at the beginning of both lists. It includes several performance optimizations: when both lists are identical (same pointer), it immediately returns the full list length; when keys1 is empty, it returns true with zero common keys; when keys2 is empty but keys1 is not, it returns false with zero common keys.

For the general case where both lists are non-empty, the function iterates through both lists simultaneously using the forboth macro, comparing pathkey pointers until a mismatch is found or one list is exhausted. The function returns true if keys1 is fully contained within keys2 (i.e., keys2 has at least as many pathkeys as keys1 and all of keys1's pathkeys match the corresponding positions in keys2). The n_common parameter receives the count of matching pathkeys from the beginning of the lists.

## Parameters / Member Variables
- `keys1`: First pathkey list (the required ordering)
- `keys2`: Second pathkey list (the available ordering)  
- `n_common`: Output parameter receiving the count of common pathkeys from the beginning

## Dependencies
- Functions called/Symbols referenced:
  - [list_length](../l/list_length.md) (gets list length for optimization)
  - forboth (iterates two lists simultaneously)
  - [PathKey](../P/PathKey.md) (pathkey structure type)
- Called from (representative examples):
  - [generate_useful_gather_paths](../g/generate_useful_gather_paths.md)
  - [pathkeys_useful_for_ordering](pathkeys_useful_for_ordering.md)
  - [pathkeys_useful_for_setop](pathkeys_useful_for_setop.md)
  - [create_window_paths](../c/create_window_paths.md)
  - [create_one_window_path](../c/create_one_window_path.md)
  - [create_ordered_paths](../c/create_ordered_paths.md)
  - [gather_grouping_paths](../g/gather_grouping_paths.md)

## Notes and Other Information
This function is particularly valuable for incremental sort planning, where knowing the exact number of common pathkeys helps determine the cost and feasibility of incremental sorting operations. The performance optimizations for identical lists and empty lists provide significant planning time improvements in worst-case scenarios. The function is widely used throughout the planner for path costing and selection decisions where partial ordering compatibility matters.

## Simplified Source

```c
bool
pathkeys_count_contained_in(List *keys1, List *keys2, int *n_common)
{
    int n = 0;
    ListCell *key1, *key2;

    // Optimization: check for special cases
    if (keys1 == keys2) {
        *n_common = list_length(keys1);
        return true;
    }
    else if (keys1 == NIL) {
        *n_common = 0;
        return true;
    }
    else if (keys2 == NIL) {
        *n_common = 0;
        return false;
    }

    // Compare both lists element by element
    forboth(key1, keys1, key2, keys2) {
        PathKey *pathkey1 = (PathKey *) lfirst(key1);
        PathKey *pathkey2 = (PathKey *) lfirst(key2);

        if (pathkey1 != pathkey2) {
            *n_common = n;
            return false;
        }
        n++;
    }

    // Return whether keys1 was fully contained (key1 exhausted)
    *n_common = n;
    return (key1 == NULL);
}
```