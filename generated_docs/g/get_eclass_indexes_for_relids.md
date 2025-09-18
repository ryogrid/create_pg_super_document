# get_eclass_indexes_for_relids

## Location
[src/backend/optimizer/path/equivclass.c:3328-3357](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/optimizer/path/equivclass.c#L3328-L3357)

## Overview
Builds and returns a Bitmapset containing the indexes into the eq_classes list for all equivalence classes that mention any of the specified relation IDs.

## Definition
```c
static Bitmapset *get_eclass_indexes_for_relids(PlannerInfo *root, Relids relids)
```

## Detailed Description
This static function creates a bitmap containing the indexes of equivalence classes that are relevant to a given set of relation IDs. It works by:

1. Iterating through each relation ID in the input relids set
2. Looking up each relation in the simple_rel_array
3. Collecting the eclass_indexes from each valid relation
4. Combining all these indexes into a single Bitmapset

The function includes special handling for outer join relations that may not have corresponding RelOptInfo entries. It asserts that equivalence class merging has been completed before execution.

## Parameters / Member Variables
- `root`: PlannerInfo structure containing global planner state and eq_classes list
- `relids`: Bitmapset of relation IDs to find relevant equivalence classes for

## Dependencies
- Functions called/Symbols referenced:
  - [bms_next_member](../b/bms_next_member.md) (bitmap set iteration function)
  - [bms_is_member](../b/bms_is_member.md) (bitmap set membership test)
  - [bms_add_members](../b/bms_add_members.md) (bitmap set union operation)
  - RelOptInfo (structure accessed for eclass_indexes field)
- Called from (representative examples):
  - [generate_join_implied_equalities](generate_join_implied_equalities.md) (src/backend/optimizer/path/equivclass.c:1423)
  - [add_child_join_rel_equivalences](../a/add_child_join_rel_equivalences.md) (src/backend/optimizer/path/equivclass.c:2767)
  - [has_relevant_eclass_joinclause](../h/has_relevant_eclass_joinclause.md) (src/backend/optimizer/path/equivclass.c:3169)
  - [get_common_eclass_indexes](get_common_eclass_indexes.md) (src/backend/optimizer/path/equivclass.c:3364, 3373)

## Notes and Other Information
- Static function only used within equivclass.c module
- Requires that ec_merging_done flag is set before invocation
- Handles outer join relations gracefully by skipping NULL RelOptInfo entries
- Used as a building block for more complex equivalence class operations
- Returns NULL if no relevant equivalence classes are found
- Part of the equivalence class indexing system for efficient lookup operations