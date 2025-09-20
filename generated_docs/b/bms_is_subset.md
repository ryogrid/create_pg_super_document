# bms_is_subset

## Location
[src/backend/nodes/bitmapset.c:412-444](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/nodes/bitmapset.c#L412-L444)

## Overview
Tests whether one bitmap set is a subset of another, returning true if all bits set in the first set are also set in the second set.

## Definition

```c
bool
bms_is_subset(const Bitmapset *a, const Bitmapset *b)
```
## Detailed Description
The function determines if bitmap set  is a subset of bitmap set  by checking that every bit position that is set in  is also set in . The implementation handles NULL input cases appropriately: a NULL set (empty set) is considered a subset of any set, while any non-empty set cannot be a subset of NULL. The function performs an early optimization by checking if  has more words than , which would immediately indicate that  cannot be a subset of . The core comparison uses bitwise AND operations with complement to efficiently detect any bits that are set in  but not in .

## Parameters / Member Variables
- : The bitmap set to test as a potential subset (can be NULL, representing an empty set)
- : The bitmap set to test as a potential superset (can be NULL, representing an empty set)

## Dependencies
- Functions called/Symbols referenced:
  - [bms_is_valid_set](bms_is_valid_set.md) (validation function for bitmap sets)
- Called from (representative examples):
  - [check_functional_grouping](../c/check_functional_grouping.md) (constraint validation)
  - [get_cheapest_parameterized_child_path](../g/get_cheapest_parameterized_child_path.md) (path optimization)
  - [initial_cost_mergejoin](../i/initial_cost_mergejoin.md) (join costing)
  - [join_is_legal](../j/join_is_legal.md) (join planning validation)
  - [clause_sides_match_join](../c/clause_sides_match_join.md) (join clause analysis)

## Notes and Other Information
This function is extensively used throughout PostgreSQL's query optimizer for testing relationships between sets of relation IDs, column numbers, and other identifiers. The subset relationship is fundamental for determining join legality, clause applicability, and path optimization. The function assumes that both input bitmap sets are valid or NULL, and uses assertions to verify this in debug builds.