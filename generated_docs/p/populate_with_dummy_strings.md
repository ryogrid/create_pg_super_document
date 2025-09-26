# populate_with_dummy_strings

## Location
src/test/modules/test_bloomfilter/test_bloomfilter.c: 32 - 51

## Overview
Populates an empty Bloom filter with a specified number of dummy string elements for testing purposes.

## Definition


## Detailed Description
This static helper function is part of PostgreSQL's Bloom filter testing module. It generates and inserts dummy string elements into a Bloom filter to simulate realistic usage scenarios during testing. The function creates sequential string elements in the format "i<number>" (e.g., "i0", "i1", "i2") and adds each one to the provided Bloom filter using the  function.

The function includes interrupt checking via  to allow for graceful cancellation during long-running test operations, which is important when dealing with large numbers of elements.

## Parameters / Member Variables
- : Pointer to the bloom_filter structure that will be populated with dummy elements
- : The number of dummy string elements to generate and add to the filter (int64 type supports very large test datasets)

## Dependencies
- Functions called/Symbols referenced:
  - bloom_add_element (adds each generated string to the Bloom filter)
  - snprintf (formats the dummy string elements)
  - strlen (calculates string length for bloom_add_element)
  - CHECK_FOR_INTERRUPTS (macro for interrupt handling)
- Called from (representative examples):
  - create_and_test_bloom

## Notes and Other Information
- The function uses  to define the buffer size for generated strings
- String format uses  for cross-platform 64-bit integer formatting
- This is a static function, meaning it's only accessible within the test_bloomfilter.c file
- The function is specifically designed for testing scenarios and generates predictable, sequential dummy data
- Interrupt checking ensures the function can be cancelled during long-running tests with large element counts