# is_input_argument

## Location
src/backend/utils/adt/ruleutils.c: 3400 - 3411

## Overview
A static helper function that determines whether a function argument at a given position is an input argument based on its mode.

## Definition
```c
static bool is_input_argument(int nth, const char *argmodes)
```

## Detailed Description
This function checks if the nth argument of a function is considered an input argument by examining the argument modes array. It returns true if the argument mode is IN, INOUT, VARIADIC, or if no argument modes are specified (which defaults to IN mode). This function is primarily used in PostgreSQL's rule utilities to determine which arguments should be considered when processing function definitions and calls.

## Parameters / Member Variables
- `nth`: The zero-based index of the argument to check
- `argmodes`: A character array containing the modes for each function argument, or NULL if no modes are specified

## Dependencies
- Functions called/Symbols referenced:
  - PROARGMODE_IN
  - PROARGMODE_INOUT  
  - PROARGMODE_VARIADIC
- Called from (representative examples):
  - [pg_get_function_arg_default](../p/pg_get_function_arg_default.md) (twice in the same function)

## Notes and Other Information
- When argmodes is NULL, the function defaults to returning true, assuming all arguments are input arguments
- This function is part of PostgreSQL's rule utilities system used for formatting and reconstructing SQL definitions
- The function only considers IN, INOUT, and VARIADIC modes as input arguments; OUT and TABLE modes are excluded