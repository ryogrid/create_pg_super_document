# check_with_filler

## Location
[src/test/modules/test_integerset/test_integerset.c:470-487](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/test/modules/test_integerset/test_integerset.c#L470-L487)

## Overview
Helper function that validates IntegerSet membership results against expected values for sets containing both a specific value and a continuous filler range.

## Definition

```c
static void
check_with_filler(IntegerSet *intset, uint64 x,
				  uint64 value, uint64 filler_min, uint64 filler_max)
```
## Detailed Description
The  function is a specialized validation helper designed to test IntegerSet membership queries in the context of sets that contain both isolated values and continuous ranges. It calculates the expected membership result for a given query value based on whether that value matches either the specific target value or falls within the defined filler range, then compares this expectation against the actual result returned by .

This function encapsulates the logic for determining expected membership in the mixed-content sets created by , providing a clean abstraction for boundary testing around critical points in the value space.

## Parameters / Member Variables
- `*intset`: Pointer to the IntegerSet being tested
- `x`: The value to query for membership in the set
- `value`: The specific isolated value that should be present in the set
- `filler_min`: Starting value (inclusive) of the continuous filler range
- `filler_max`: Ending value (exclusive) of the continuous filler range
## Dependencies
- Functions called/Symbols referenced:
  - [IntegerSet](../I/IntegerSet.md) (data structure)
  - [intset_is_member](../i/intset_is_member.md)
  - UINT64_FORMAT
  - elog (with ERROR level)
- Called from (representative examples):
  - [test_single_value_and_filler](../t/test_single_value_and_filler.md) (called 13 times with different boundary values)

## Notes and Other Information
- This is a static function, only accessible within the test_integerset.c file
- Implements the membership logic: value is present if (x == value) OR (filler_min <= x < filler_max)
- Critical for systematic boundary testing in mixed sparse/dense IntegerSet configurations
- Used extensively by test_single_value_and_filler() to validate membership at edge cases
- Provides clear error reporting with the specific value that failed the membership test
- Essential for ensuring IntegerSet correctness when dealing with both isolated and continuous value patterns
- Located in: src/test/modules/test_integerset/test_integerset.c:470-487
- Abstracts away the expected value calculation logic for cleaner test code

## Simplified Source

```c
static void
check_with_filler(IntegerSet *intset, uint64 x,
                  uint64 value, uint64 filler_min, uint64 filler_max)
{
    // Calculate expected membership: either the specific value or within filler range
    bool expected = (x == value || (filler_min <= x && x < filler_max));

    // Check actual membership result
    bool actual = intset_is_member(intset, x);

    // Verify they match
    if (actual != expected) {
        elog(ERROR, "intset_is_member failed for %llu", x);
    }
}
```