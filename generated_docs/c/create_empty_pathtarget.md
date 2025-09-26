# create_empty_pathtarget

## Location
[src/backend/optimizer/util/tlist.c:681-694](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/optimizer/util/tlist.c#L681-L694)

## Overview
Creates an empty PathTarget structure with zero columns and zero cost, serving as a foundation for building PathTarget structures incrementally.

## Definition
```c
PathTarget *create_empty_pathtarget(void)
```

## Detailed Description
This function creates a completely empty PathTarget structure using makeNode(PathTarget). The resulting PathTarget has no expressions, no sortgrouprefs, zero cost, zero width, and unknown volatility status. This serves as a convenient starting point for functions that need to build PathTarget structures incrementally by adding columns one at a time.

The function intentionally encapsulates the simple makeNode() call to provide a stable API that doesn't expose the internal implementation details to callers. This allows the implementation to change if needed without affecting calling code.

## Parameters / Member Variables


## Dependencies
- Functions called/Symbols referenced:
  - [PathTarget](../P/PathTarget.md) (data structure)
  - makeNode (node creation)
- Called from (representative examples):
  - [make_group_input_target](../m/make_group_input_target.md)
  - [make_partial_grouping_target](../m/make_partial_grouping_target.md)
  - [make_window_input_target](../m/make_window_input_target.md)
  - [make_sort_input_target](../m/make_sort_input_target.md)
  - [build_simple_rel](../b/build_simple_rel.md)
  - [build_join_rel](../b/build_join_rel.md)
  - build_child_join_rel
  - [fetch_upper_rel](../f/fetch_upper_rel.md)
  - [split_pathtarget_at_srfs](../s/split_pathtarget_at_srfs.md)

## Notes and Other Information
- Returns a PathTarget with no expressions and zero cost/width
- Provides a stable API that doesn't expose makeNode() implementation details to callers
- Commonly used as a starting point for incrementally building PathTarget structures
- The returned PathTarget has VOLATILITY_UNKNOWN status initially
- All fields are initialized to their default values by makeNode()
- The function is declared in src/include/optimizer/tlist.h