# find_simple

## Location
[src/interfaces/ecpg/preproc/variable.c:177-192](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/interfaces/ecpg/preproc/variable.c#L177-L192)

## Overview
Performs a linear search through the global variable list to find and return a variable by its exact name.

## Definition

```c
struct variable *p;
```
## Detailed Description
This function implements a straightforward variable lookup mechanism in the ECPG preprocessor. It iterates through the global linked list of all variables (allvariables) and performs string comparison to find a variable with the exact matching name. This is the foundation for variable resolution in ECPG's embedded SQL processing.

The function uses a simple linear search algorithm, comparing each variable's name using strcmp until a match is found or the end of the list is reached. This serves as the base case for more complex variable resolution operations that may involve struct members, array indexing, or pointer dereferencing.

## Parameters / Member Variables
- : The exact variable name to search for

## Dependencies
- Functions called/Symbols referenced:
  - strcmp (C library function for string comparison)
  - allvariables (global variable list)

- Called from (representative examples):
  - [find_variable](find_variable.md) (main variable resolution function)

## Notes and Other Information
- This is a static function, only accessible within the variable.c file
- Returns NULL if no variable with the specified name is found
- Uses linear search with O(n) time complexity where n is the number of variables
- Forms the base layer of ECPG's variable resolution hierarchy
- The allvariables global list contains all variables known to the ECPG preprocessor
- Simple and reliable implementation suitable for typical embedded SQL variable counts