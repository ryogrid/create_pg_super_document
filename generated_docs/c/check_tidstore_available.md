# check_tidstore_available

## Location
[src/test/modules/test_tidstore/test_tidstore.c:150-156](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/test/modules/test_tidstore/test_tidstore.c#L150-L156)

## Overview
A static utility function that verifies the tidstore has been properly initialized before performing any operations on it.

## Definition

```c
static void
check_tidstore_available(void)
```
## Detailed Description
This function performs a simple but critical validation check to ensure that the global tidstore variable has been initialized before any tidstore operations are attempted. It serves as a guard function to prevent operations on uninitialized tidstore instances, which would cause crashes or undefined behavior.

The function checks if the global tidstore pointer is NULL and immediately throws a PostgreSQL error if it hasn't been initialized. This provides clear, immediate feedback when test functions are called in the wrong order or when the tidstore creation step has been skipped.

## Parameters / Member Variables
- This function takes no parameters (void)

## Dependencies
- Functions called/Symbols referenced:
  - elog (for error logging and exception throwing)
- Called from (representative examples):
  - [do_set_block_offsets](../d/do_set_block_offsets.md)
  - [check_set_block_offsets](check_set_block_offsets.md)
  - [test_is_full](../t/test_is_full.md)
  - [test_destroy](../t/test_destroy.md)

## Notes and Other Information
- This is a static helper function used internally within the test_tidstore module
- Acts as a precondition check for all tidstore operations in the test framework
- Uses elog with ERROR level, which throws a PostgreSQL exception and aborts the current transaction
- Essential for maintaining proper test execution order and preventing crashes from uninitialized state
- Simple but effective defensive programming practice that makes debugging test issues much easier
- The global tidstore variable is expected to be initialized by the test_create function before any other operations