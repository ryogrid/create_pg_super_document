# test_single_value_and_filler

## Location
[src/test/modules/test_integerset/test_integerset.c:377-469](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/test/modules/test_integerset/test_integerset.c#L377-L469)

## Overview
Test function that validates IntegerSet behavior with both a single specific value and a continuous range of filler values, exercising internal buffering and B-tree codepaths.

## Definition


## Detailed Description
The  function performs comprehensive testing of the PostgreSQL IntegerSet implementation by creating sets that contain both a specific target value and a continuous range of filler values. This testing approach is specifically designed to exercise different internal codepaths than single-value tests, particularly the internal B-tree implementation and buffering mechanisms that are only triggered when the set contains multiple values.

The function strategically adds values in a specific order: the target value is added either before or after the filler range depending on its position relative to the range boundaries. This tests the IntegerSet's ability to handle values both within and outside of continuous ranges. The function then performs extensive validation using the helper function  to test membership queries around critical boundary points, and validates iteration order and completeness.

## Parameters / Member Variables
- : The specific uint64 value to test in isolation within the set
- : Starting value (inclusive) of the continuous range to add as filler
- : Ending value (exclusive) of the continuous range to add as filler

## Dependencies
- Functions called/Symbols referenced:
  - [IntegerSet](../I/IntegerSet.md) (data structure)
  - [intset_create](../i/intset_create.md)
  - [intset_add_member](../i/intset_add_member.md)
  - [intset_num_entries](../i/intset_num_entries.md)
  - [intset_begin_iterate](../i/intset_begin_iterate.md)
  - [intset_iterate_next](../i/intset_iterate_next.md)
  - [intset_memory_usage](../i/intset_memory_usage.md)
  - [check_with_filler](../c/check_with_filler.md) (helper function for boundary testing)
  - PG_UINT64_MAX
  - UINT64_FORMAT
  - NOTICE (logging level)
  - [palloc](../p/palloc.md)
- Called from (representative examples):
  - [test_integerset](test_integerset.md) (called multiple times with various boundary value combinations)

## Notes and Other Information
- This is a static function, only accessible within the test_integerset.c file
- Specifically designed to test internal B-tree codepaths that aren't exercised by single-value tests
- Uses extensive boundary testing with check_with_filler() to validate membership at critical points
- Tests values at and around: 0, 1, filler_min±1, value±1, filler_max±1, PG_UINT64_MAX±1
- Validates that iteration returns values in correct sorted order
- Includes sanity checking of memory usage (must be between 5KB and 500MB)
- The filler range exercises buffering mechanisms within the IntegerSet implementation
- Called from test_integerset() with various edge case combinations to ensure robustness
- Located in: src/test/modules/test_integerset/test_integerset.c:377-469
- Critical for testing IntegerSet behavior with mixed sparse and dense value patterns