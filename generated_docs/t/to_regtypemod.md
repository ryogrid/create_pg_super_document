# to_regtypemod

## Location
[src/backend/utils/adt/regproc.c:1229-1246](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/regproc.c#L1229-L1246)

## Overview
Converts a textual type name to its type modifier value, returning NULL if the type name is not found.

## Definition

```c
Datum
to_regtypemod(PG_FUNCTION_ARGS)
```
## Detailed Description
The  function is a PostgreSQL built-in function that takes a text representation of a type name and returns its corresponding type modifier value. The function uses the internal  function to parse the input string and extract both the type OID and type modifier. If the parsing fails (e.g., the type name is invalid or not found), the function returns NULL instead of throwing an error.

This function is part of PostgreSQL's regtype family of functions that provide safe type name to identifier conversions. Unlike functions that might raise errors on invalid input,  gracefully handles invalid type names by returning NULL.

## Parameters / Member Variables
- : Standard PostgreSQL function argument macro that provides access to function arguments
  - First argument (index 0): Text representation of the type name to be converted

## Dependencies
- Functions called/Symbols referenced:
  - : Converts PostgreSQL text type to C string
  - : Error handling context structure
  - : Parses type string into type OID and modifier
- Called from (representative examples):
  - No direct references found in the codebase

## Notes and Other Information
- Returns NULL for invalid type names rather than raising an error
- Uses ErrorSaveContext to handle parsing errors gracefully
- Part of PostgreSQL's regtype function family
- Located in src/backend/utils/adt/regproc.c
- The function extracts only the type modifier part, discarding the type OID that parseTypeString also provides