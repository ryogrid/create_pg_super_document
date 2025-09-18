# fmgr_lookupByName

## Location
[src/backend/utils/fmgr/fmgr.c:101-126](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/fmgr/fmgr.c#L101-L126)

## Overview
A lookup function that searches the builtin function table by function name to locate PostgreSQL builtin functions when the Oid is not available.

## Definition
```c
static const FmgrBuiltin *fmgr_lookupByName(const char *name)
```

## Detailed Description
fmgr_lookupByName performs a linear search through PostgreSQL's builtin function table using a function's name. Unlike fmgr_isbuiltin which uses Oids for O(1) lookup, this function must iterate through the entire builtin function array to find a matching name. The function uses string comparison to match the provided name against the funcName field of each FmgrBuiltin entry.

This lookup method is slower than Oid-based lookup but is necessary when only the function name is known. The function can handle cases where multiple entries in the array have the same name, as they should all point to the same underlying routine. It returns the first matching entry found.

## Parameters / Member Variables
- `name`: The name of the function to look up in the builtin function table as a null-terminated C string

## Dependencies
- Functions called/Symbols referenced:
  - strcmp (standard C library function for string comparison)
  - fmgr_nbuiltins (global variable containing the count of builtin functions)
  - fmgr_builtins (global array containing builtin function metadata)
- Called from (representative examples):
  - [fmgr_info_cxt_security](fmgr_info_cxt_security.md) (when setting up function call information with security context using function name)
  - [fmgr_internal_function](fmgr_internal_function.md) (when looking up internal functions by name)

## Notes and Other Information
- This is a static function, only accessible within the fmgr.c file
- Performs O(n) linear search, making it slower than Oid-based lookup
- Returns NULL if no matching function name is found
- Can handle duplicate function names in the builtin table, which may exist for different argument combinations
- Part of PostgreSQL's Function Manager (fmgr) subsystem responsible for function call dispatch
- Used primarily when function names are known but Oids are not available, such as during internal function resolution