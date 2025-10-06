# test_single_value_and_filler

## Location
[src/test/modules/test_integerset/test_integerset.c:377-469](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/test/modules/test_integerset/test_integerset.c#L377-L469)

## Overview
Test function that validates IntegerSet behavior with both a single specific value and a continuous range of filler values, exercising internal buffering and B-tree codepaths.

## Definition

```c
static void
test_single_value_and_filler(uint64 value, uint64 filler_min, uint64 filler_max)
```
## Detailed Description
The  function performs comprehensive testing of the PostgreSQL IntegerSet implementation by creating sets that contain both a specific target value and a continuous range of filler values. This testing approach is specifically designed to exercise different internal codepaths than single-value tests, particularly the internal B-tree implementation and buffering mechanisms that are only triggered when the set contains multiple values.

The function strategically adds values in a specific order: the target value is added either before or after the filler range depending on its position relative to the range boundaries. This tests the IntegerSet's ability to handle values both within and outside of continuous ranges. The function then performs extensive validation using the helper function  to test membership queries around critical boundary points, and validates iteration order and completeness.

## Parameters / Member Variables
- `value`: The specific uint64 value to test in isolation within the set
- `filler_min`: Starting value (inclusive) of the continuous range to add as filler
- `filler_max`: Ending value (exclusive) of the continuous range to add as filler
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

## Simplified Source

```c
static void
test_single_value_and_filler(uint64 value, uint64 filler_min, uint64 filler_max)
{
    IntegerSet *intset;
    uint64 x;
    bool found;
    uint64 *iter_expected;
    uint64 n = 0;

    elog(NOTICE, "testing intset with value %llu, and all between %llu and %llu",
         value, filler_min, filler_max);

    intset = intset_create();
    iter_expected = palloc(sizeof(uint64) * (filler_max - filler_min + 1));

    // Add values in order: single value first if before range
    if (value < filler_min) {
        intset_add_member(intset, value);
        iter_expected[n++] = value;
    }

    // Add the continuous filler range
    for (x = filler_min; x < filler_max; x++) {
        intset_add_member(intset, x);
        iter_expected[n++] = x;
    }

    // Add single value after range if needed
    if (value >= filler_max) {
        intset_add_member(intset, value);
        iter_expected[n++] = value;
    }

    // Verify the entry count is correct
    if (intset_num_entries(intset) != n) {
        elog(ERROR, "intset_num_entries mismatch");
    }

    // Test membership at critical boundary points
    check_with_filler(intset, 0, value, filler_min, filler_max);
    check_with_filler(intset, 1, value, filler_min, filler_max);
    check_with_filler(intset, filler_min - 1, value, filler_min, filler_max);
    check_with_filler(intset, filler_min, value, filler_min, filler_max);
    check_with_filler(intset, value, value, filler_min, filler_max);
    check_with_filler(intset, filler_max, value, filler_min, filler_max);
    check_with_filler(intset, PG_UINT64_MAX, value, filler_min, filler_max);

    // Verify iteration returns values in correct order
    intset_begin_iterate(intset);
    for (uint64 i = 0; i < n; i++) {
        found = intset_iterate_next(intset, &x);
        if (!found || x != iter_expected[i]) {
            elog(ERROR, "intset_iterate_next failed");
        }
    }

    // Verify no extra values at end of iteration
    if (intset_iterate_next(intset, &x)) {
        elog(ERROR, "unexpected extra value in iteration");
    }

    // Sanity check memory usage
    uint64 mem_usage = intset_memory_usage(intset);
    if (mem_usage < 5000 || mem_usage > 500000000) {
        elog(ERROR, "suspicious memory usage: %llu", mem_usage);
    }
}
```