# bms_overlap

## Location
[src/backend/nodes/bitmapset.c:582-607](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/nodes/bitmapset.c#L582-L607)

## Overview
Tests whether two bitmap sets have any common members by checking if their intersection is non-empty.

## Definition

```c
bool
bms_overlap(const Bitmapset *a, const Bitmapset *b)
```
## Detailed Description
This function determines if two bitmap sets have any bits in common by performing a bitwise AND operation on corresponding words and checking if any result is non-zero. The function handles NULL inputs by treating them as empty sets, which means any comparison involving a NULL set will return false (empty sets cannot overlap with anything). The implementation is optimized to only check the shorter of the two sets, since any bits beyond the shorter set's range cannot contribute to an overlap. The function returns immediately upon finding the first overlapping bit, making it efficient for cases where sets have early overlaps.

## Parameters / Member Variables
- `a`: The first bitmap set to test for overlap (can be NULL, representing an empty set)
- `b`: The second bitmap set to test for overlap (can be NULL, representing an empty set)

## Dependencies
- Functions called/Symbols referenced:
  - [bms_is_valid_set](bms_is_valid_set.md) (validation function for bitmap sets)
- Called from (representative examples):
  - [heap_update](../h/heap_update.md) (heap access method for update operations)
  - [has_partition_attrs](../h/has_partition_attrs.md) (partitioning attribute analysis)
  - [ExecUpdateLockMode](../E/ExecUpdateLockMode.md) (executor lock mode determination)
  - [generate_join_implied_equalities](../g/generate_join_implied_equalities.md) (join equality generation)
  - [join_is_legal](../j/join_is_legal.md) (join legality checking)

## Notes and Other Information
This function is extensively used throughout PostgreSQL's query optimizer and execution engine to test for conflicts, dependencies, and relationships between sets of identifiers. Common use cases include checking if two relations share attributes, if join conditions affect overlapping column sets, if outer join conditions conflict with other constraints, and if parameter dependencies exist between different parts of a query plan. The function's efficiency is crucial since it's called frequently during query planning and execution, particularly in complex join scenarios and partitioned table operations.