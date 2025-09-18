# append_variable_names

## Location
[src/bin/psql/tab-complete.c:5730-5752](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/bin/psql/tab-complete.c#L5730-L5752)

## Overview
Appends a variable name with optional prefix and suffix to a dynamically growing array of variable names used in PostgreSQL's psql tab completion system.

## Definition
```c
static void append_variable_names(char ***varnames, int *nvars, int *maxvars, const char *varname, const char *prefix, const char *suffix)
```

## Detailed Description
This function is a utility for managing dynamic arrays of variable names in PostgreSQL's psql tab completion system. It handles the dynamic resizing of the variable names array when capacity is exceeded, doubling the array size as needed. The function constructs a formatted string by concatenating the prefix, variable name, and suffix, then adds it to the array while updating the count of variables.

## Parameters / Member Variables
- `varnames`: Pointer to the array of variable name strings (passed by reference for resizing)
- `nvars`: Pointer to the current number of variables in the array (updated by function)
- `maxvars`: Pointer to the maximum capacity of the array (updated when resizing)
- `varname`: The core variable name to be added
- `prefix`: String to prepend to the variable name (can be empty)
- `suffix`: String to append to the variable name (can be empty)

## Dependencies
- Functions called/Symbols referenced:
  - pg_realloc (for resizing the variable names array)
  - [psprintf](../p/psprintf.md) (for formatting the complete variable name string)
- Called from (representative examples):
  - THING_NO_SHOW macro usage
  - [complete_from_variables](../c/complete_from_variables.md) function

## Notes and Other Information
- Implements a dynamic array growth strategy, doubling capacity when needed
- The array is NULL-terminated as indicated by the (+1) in the realloc size calculation
- Memory management is handled automatically through pg_realloc
- The formatted strings created by psprintf are owned by the array and should be freed when the array is deallocated
- Used specifically for building lists of psql variables for tab completion
- The prefix and suffix parameters allow for flexible formatting of variable names (e.g., adding quotes or special characters)