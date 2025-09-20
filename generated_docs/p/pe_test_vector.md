# pe_test_vector

## Location
[src/test/modules/test_escape/test_escape.c:76-81](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/test/modules/test_escape/test_escape.c#L76-L81)

## Overview
A structure that represents a single test case input for PostgreSQL escape function testing, containing the client encoding context and escape sequence data.

## Definition

```c
typedef struct pe_test_vector
{
	const char *client_encoding;
	size_t		escape_len;
	const char *escape;
} pe_test_vector;
```
## Detailed Description
The  structure encapsulates a single test input case for the escape function testing framework. Each test vector defines a specific scenario with a particular client encoding and an escape sequence to be tested. This structure allows the test framework to systematically test escape functions against various encoding contexts and input patterns, ensuring comprehensive coverage of different character encoding scenarios that might be encountered in real-world PostgreSQL usage.

## Parameters / Member Variables
- `*client_encoding`: String identifier for the client character encoding context in which this test should be executed
- `escape_len`: Length in bytes of the escape sequence data
- `*escape`: Pointer to the actual escape sequence data to be used as test input
## Dependencies
- Functions called/Symbols referenced:
  - (No direct references from this structure)
- Called from (representative examples):
  - TV_LEN (macro for calculating test vector length)
  - [test_one_vector_escape](../t/test_one_vector_escape.md)
  - [test_one_vector](../t/test_one_vector.md)

## Notes and Other Information
This structure is fundamental to the test framework's data-driven testing approach, allowing test cases to be defined declaratively as arrays of test vectors. The inclusion of both length and data pointer supports testing with binary data that may contain null bytes, which is important for comprehensive escape function validation across different character encodings and edge cases.