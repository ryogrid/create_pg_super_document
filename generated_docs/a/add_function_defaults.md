# add_function_defaults

## Location
src/backend/optimizer/util/clauses.c: 4326 - 4349

## Overview
Appends missing function arguments with their default values when the argument list contains only positional arguments.

## Definition


## Detailed Description
This function handles the simple case where a function call uses only positional arguments but provides fewer arguments than the function expects. It assumes that all missing arguments are at the end of the argument list and can be filled in with consecutive default values.

The function operates by:
1. **Fetching defaults**: Retrieves all default argument expressions for the function
2. **Calculating needed defaults**: Determines how many default arguments are actually needed
3. **Trimming excess defaults**: Removes unused default expressions from the beginning of the defaults list
4. **Combining lists**: Concatenates the provided arguments with the needed default expressions

This is a simpler operation compared to reorder_function_arguments since it doesn't need to handle named arguments or complex reordering scenarios.

## Parameters / Member Variables
- : Input list of positional arguments provided to the function
- : Total number of parameters the function expects
- : The function's pg_proc tuple for accessing default values

## Dependencies
- Functions called/Symbols referenced:
  - fetch_function_defaults
  - list_delete_first_n
  - list_concat_copy
- Called from (representative examples):
  - expand_function_arguments

## Notes and Other Information
- This function is used only for positional argument lists, not for mixed positional/named arguments
- The function assumes that missing arguments are consecutive at the end of the parameter list
- Input validation ensures there are enough default values available for the missing arguments
- The function creates a new combined list without modifying the original argument list
- Default expressions are trimmed from the front when more defaults are available than needed
- An error is raised if insufficient default arguments are available to fill all missing positions