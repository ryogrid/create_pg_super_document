# check_with_filler

## Location
src/test/modules/test_integerset/test_integerset.c: 470 - 487

## Overview
Helper function that validates IntegerSet membership results against expected values for sets containing both a specific value and a continuous filler range.

## Definition


## Detailed Description
The  function is a specialized validation helper designed to test IntegerSet membership queries in the context of sets that contain both isolated values and continuous ranges. It calculates the expected membership result for a given query value based on whether that value matches either the specific target value or falls within the defined filler range, then compares this expectation against the actual result returned by .

This function encapsulates the logic for determining expected membership in the mixed-content sets created by , providing a clean abstraction for boundary testing around critical points in the value space.

## Parameters / Member Variables
- : Pointer to the IntegerSet being tested
- : The value to query for membership in the set
- : The specific isolated value that should be present in the set
- : Starting value (inclusive) of the continuous filler range
- : Ending value (exclusive) of the continuous filler range

## Dependencies
- Functions called/Symbols referenced:
  - IntegerSet (data structure)
  - intset_is_member
  - UINT64_FORMAT
  - elog (with ERROR level)
- Called from (representative examples):
  - test_single_value_and_filler (called 13 times with different boundary values)

## Notes and Other Information
- This is a static function, only accessible within the test_integerset.c file
- Implements the membership logic: value is present if (x == value) OR (filler_min <= x < filler_max)
- Critical for systematic boundary testing in mixed sparse/dense IntegerSet configurations
- Used extensively by test_single_value_and_filler() to validate membership at edge cases
- Provides clear error reporting with the specific value that failed the membership test
- Essential for ensuring IntegerSet correctness when dealing with both isolated and continuous value patterns
- Located in: src/test/modules/test_integerset/test_integerset.c:470-487
- Abstracts away the expected value calculation logic for cleaner test code