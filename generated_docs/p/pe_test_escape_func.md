# pe_test_escape_func

## Location
src/test/modules/test_escape/test_escape.c: 41 - 71

## Overview
A structure that defines the interface and capabilities of PostgreSQL escape functions to be tested by the test_escape module.

## Definition


## Detailed Description
The  structure encapsulates metadata and function pointer for different PostgreSQL escape functions that need to be tested. It serves as a standardized interface that allows the test framework to understand the capabilities and limitations of each escape function, enabling appropriate test case selection and validation. The structure includes capability flags that inform the test framework about what types of input the escape function can handle and what behavior to expect.

## Parameters / Member Variables
- : Human-readable name identifier for the escape function
- : Flag indicating whether the escape method can report errors and should be tested for error handling with invalid inputs
- : Flag indicating the escape method only handles validly encoded input; tests are skipped unless --force-unsupported is used
- : Flag indicating the escape method only handles encodings where no byte in multi-byte characters are valid ASCII
- : Flag indicating whether the escape function accepts a length parameter for input
- : Function pointer to the actual escape function being tested, with standardized signature taking connection, target buffer, input string, input length, and error buffer

## Dependencies
- Functions called/Symbols referenced:
  - (No direct references from this structure)
- Called from (representative examples):
  - [escape_fmt_id](../e/escape_fmt_id.md)
  - [test_one_vector_escape](../t/test_one_vector_escape.md)
  - [test_one_vector](../t/test_one_vector.md)

## Notes and Other Information
This structure is crucial for the test framework's ability to handle different escape functions uniformly while respecting their individual capabilities and limitations. The capability flags allow the test framework to make intelligent decisions about which tests to run and what results to expect, ensuring comprehensive but appropriate testing coverage for each escape function type.