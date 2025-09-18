# GetVariable

## Location
src/bin/psql/variables.c: 71 - 106

## Overview
Retrieves the string value of a variable from a variable space, or returns NULL if the variable is not defined.

## Definition


## Detailed Description
This function searches for a variable by name within the specified variable space and returns its string value. The function performs a linear search through the linked list of variables, taking advantage of the fact that variables are stored in alphabetical order by name. This allows for early termination of the search when the comparison indicates the target variable cannot exist in the remaining list.

The function handles both defined variables with NULL values and completely undefined variables by returning NULL in both cases. The returned string pointer is valid until the variable is next assigned to, meaning callers should not store the pointer for extended periods without copying the string content.

## Parameters / Member Variables
- : The VariableSpace to search within. If NULL, the function returns NULL immediately
- : The name of the variable to retrieve. Must be a null-terminated string

## Dependencies
- Functions called/Symbols referenced:
  - VariableSpace (typedef)
  - struct _variable (internal variable structure)
  - strcmp (standard C library function)
- Called from (representative examples):
  - psql_get_variable
  - initializeInput
  - MAX_PROMPT_SIZE

## Notes and Other Information
- Returns NULL for both undefined variables and variables explicitly set to NULL
- The returned pointer should not be modified by the caller
- The search is optimized by the alphabetical ordering of variables in the list
- Result validity is limited to the period before the next assignment to the variable
- Part of psql's core variable management system used throughout the application