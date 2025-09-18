# get_common_eclass_indexes

## Location
src/backend/optimizer/path/equivclass.c: 3358 - 3377

## Overview
Builds and returns a Bitmapset containing the indexes of equivalence classes that mention relations in both of two specified relation ID sets.

## Definition
```c
static Bitmapset *get_common_eclass_indexes(PlannerInfo *root, Relids relids1, Relids relids2)
```

## Detailed Description
This static function finds the intersection of equivalence classes that are relevant to two different sets of relation IDs. It efficiently computes which equivalence classes involve relations from both input sets, which is useful for determining potential join conditions.

The function includes an optimization: when the second relation set contains only a single relation, it directly uses that relation's eclass_indexes rather than calling the more general get_eclass_indexes_for_relids function. The final result is computed using bitmap intersection (bms_int_members), recycling the first input bitmap for memory efficiency.

## Parameters / Member Variables
- `root`: PlannerInfo structure containing global planner state and eq_classes list
- `relids1`: First set of relation IDs to find common equivalence classes for
- `relids2`: Second set of relation IDs to find common equivalence classes for

## Dependencies
- Functions called/Symbols referenced:
  - [get_eclass_indexes_for_relids](get_eclass_indexes_for_relids.md) (helper function to get EC indexes for relation sets)
  - [bms_get_singleton_member](../b/bms_get_singleton_member.md) (bitmap set singleton detection and extraction)
  - [bms_int_members](../b/bms_int_members.md) (bitmap set intersection operation)
  - RelOptInfo (structure accessed for eclass_indexes field)
- Called from (representative examples):
  - [generate_join_implied_equalities](generate_join_implied_equalities.md) (src/backend/optimizer/path/equivclass.c:1425)
  - [have_relevant_eclass_joinclause](../h/have_relevant_eclass_joinclause.md) (src/backend/optimizer/path/equivclass.c:3103)

## Notes and Other Information
- Static function only used within equivclass.c module
- Optimized for the common case where one of the relation sets is a singleton
- Uses bitmap intersection to efficiently find common equivalence classes
- Memory efficient by recycling the first input bitmap for the result
- Essential for join planning to identify relevant equivalence classes between relation sets
- Returns the intersection of equivalence classes that span both relation sets