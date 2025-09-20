# test_single_value

## Location
[src/test/modules/test_integerset/test_integerset.c:321-376](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/test/modules/test_integerset/test_integerset.c#L321-L376)

## Overview
Test function that validates IntegerSet operations with a single integer value, testing boundary conditions and basic functionality.

## Definition

```c
static void
test_single_value(uint64 value)
```
## Detailed Description
The  function performs focused testing of the PostgreSQL IntegerSet data structure when it contains exactly one integer value. This function is crucial for validating edge cases and ensuring that the IntegerSet implementation correctly handles single-element sets across the entire uint64 range. It systematically tests all core IntegerSet operations including creation, insertion, membership queries, entry counting, and iteration.

The function creates an IntegerSet, adds the specified value, and then performs comprehensive validation. It tests membership queries for special boundary values (0, 1, PG_UINT64_MAX) as well as the actual inserted value. The function also validates that the iterator correctly returns exactly one value and then properly indicates end-of-iteration.

## Parameters / Member Variables
- : The uint64 integer value to be added to and tested in the IntegerSet

## Dependencies
- Functions called/Symbols referenced:
  - [IntegerSet](../I/IntegerSet.md) (data structure)
  - [intset_create](../i/intset_create.md)
  - [intset_add_member](../i/intset_add_member.md)
  - [intset_num_entries](../i/intset_num_entries.md)
  - [intset_is_member](../i/intset_is_member.md)
  - [intset_begin_iterate](../i/intset_begin_iterate.md)
  - [intset_iterate_next](../i/intset_iterate_next.md)
  - PG_UINT64_MAX
  - UINT64_FORMAT
  - NOTICE (logging level)
- Called from (representative examples):
  - [test_integerset](test_integerset.md) (called multiple times with boundary values: 0, 1, PG_UINT64_MAX-1, PG_UINT64_MAX)

## Notes and Other Information
- This is a static function, only accessible within the test_integerset.c file
- Specifically tests boundary conditions by checking membership for 0, 1, and PG_UINT64_MAX
- Validates that intset_num_entries() correctly returns 1 for single-element sets
- Ensures iterator behavior is correct: returns exactly one value then indicates completion
- Called from test_integerset() with various edge case values to test the full uint64 range
- Essential for verifying IntegerSet correctness with minimal data sets
- Located in: src/test/modules/test_integerset/test_integerset.c:321-376
- Part of PostgreSQL's comprehensive IntegerSet test suite