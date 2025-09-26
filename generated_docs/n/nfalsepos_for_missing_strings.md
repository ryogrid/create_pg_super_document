# nfalsepos_for_missing_strings

## Location
src/test/modules/test_bloomfilter/test_bloomfilter.c: 52 - 71

## Overview
Counts the number of false positive results when testing elements that were never added to a Bloom filter.

## Definition
```c
static int64 nfalsepos_for_missing_strings(bloom_filter *filter, int64 nelements)
```

## Detailed Description
This static helper function is part of PostgreSQL's Bloom filter testing module and is designed to measure the false positive rate of a Bloom filter. It generates dummy string elements that were never added to the filter (using a different prefix "M" instead of "i") and tests whether the Bloom filter incorrectly reports them as potentially present.

The function systematically generates nelements test strings in the format "M<number>" (e.g., "M0", "M1", "M2") and uses `bloom_lacks_element` to test each one. Since these strings were never added to the filter, any result other than "definitely absent" represents a false positive. The function counts and returns the total number of such false positives.

This measurement is crucial for validating that the Bloom filter's false positive rate matches theoretical expectations and helps ensure the filter is functioning correctly.

## Parameters / Member Variables
- `filter`: Pointer to the bloom_filter structure to test for false positives
- `nelements`: The number of test strings to generate and check against the filter

## Dependencies
- Functions called/Symbols referenced:
  - bloom_lacks_element (tests whether elements are definitely absent from the filter)
  - snprintf (formats the test string elements) 
  - strlen (calculates string length for bloom_lacks_element)
  - CHECK_FOR_INTERRUPTS (macro for interrupt handling)
- Called from (representative examples):
  - create_and_test_bloom

## Notes and Other Information
- Uses "M" prefix for test strings to ensure they differ from the "i" prefix used by populate_with_dummy_strings
- The function uses `MAX_ELEMENT_BYTES` to define the buffer size for generated strings
- String format uses `INT64_FORMAT` for cross-platform 64-bit integer formatting
- This is a static function, only accessible within the test_bloomfilter.c file
- The return value represents the observed false positive count, which can be compared against theoretical expectations
- Interrupt checking ensures the function can be cancelled during long-running tests
- False positives are expected behavior in Bloom filters, but their rate should match design parameters