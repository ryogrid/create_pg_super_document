# injection_error

## Location
[src/test/modules/injection_points/injection_points.c:178-188](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/test/modules/injection_points/injection_points.c#L178-L188)

## Overview
A public callback function that triggers an ERROR-level log message when attached to an injection point, used for testing error handling scenarios.

## Definition


## Detailed Description
This function serves as one of the available callback functions that can be attached to injection points in PostgreSQL's testing framework. When triggered, it evaluates the associated condition and, if allowed, generates an ERROR-level log message using PostgreSQL's  mechanism. This function is specifically designed to simulate error conditions during testing, allowing developers to verify that error handling paths work correctly under controlled conditions.

## Parameters / Member Variables
- : String name of the injection point that triggered this callback
- : Generic pointer to condition data, cast to  structure

## Dependencies
- Functions called/Symbols referenced:
  -  (struct type cast from private_data)
  -  (function to evaluate whether the injection point should execute)
  -  (PostgreSQL logging function - generates ERROR level message)
- Called from:
  - No direct callers found (likely registered as callback and invoked via injection point framework)

## Notes and Other Information
- This is a public function (non-static), making it available for use outside injection_points.c
- Part of a set of callback functions available for injection points (as noted in the source comment)
- The ERROR level log message will cause the current transaction to abort and may terminate the process depending on context
- Uses the standard injection point callback signature with name and private_data parameters
- The private_data is expected to contain an  structure for conditional execution
- Critical for testing error recovery, transaction rollback, and cleanup mechanisms in PostgreSQL
- The generated error message includes the injection point name for debugging purposes