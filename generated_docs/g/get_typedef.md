# get_typedef

## Location
[src/interfaces/ecpg/preproc/variable.c:498-514](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/interfaces/ecpg/preproc/variable.c#L498-L514)

## Overview
Searches the global typedef list to find and return a typedef structure by name, with optional error handling for unrecognized type names.

## Definition


## Detailed Description
The `get_typedef` function performs a linear search through the global linked list of typedef definitions (`types`) to locate a typedef structure matching the specified name. This function is essential for type resolution in the ECPG preprocessor, allowing the system to resolve user-defined type names during SQL statement preprocessing.

The function provides flexible error handling through the `noerror` parameter. When `noerror` is false, the function will terminate the program with a fatal error if the typedef is not found. When `noerror` is true, it returns NULL for missing typedefs, allowing the caller to handle the absence gracefully.

## Parameters / Member Variables
- `name`: The name of the typedef to search for as a constant character string
- `noerror`: Boolean flag controlling error behavior - if false, missing typedefs cause fatal errors; if true, NULL is returned for missing typedefs

## Dependencies
- Functions called/Symbols referenced:
  - typedefs: Global linked list structure containing typedef definitions
  - mmfatal: Fatal error reporting function that terminates execution
  - PARSE_ERROR: Error code constant for parsing errors
- Called from (representative examples):
  - (No direct references found in the analyzed codebase, but likely used during type parsing and resolution)

## Notes and Other Information
- Uses simple string comparison (`strcmp`) for typedef name matching
- Performs linear search through the global typedef list, which may be inefficient for large numbers of typedefs
- The global `types` variable maintains the linked list of all typedef definitions
- Error messages include the unrecognized type name for better debugging
- Return value of NULL when `noerror` is true allows for optional typedef resolution
- Part of the ECPG type system that manages user-defined types in embedded SQL code