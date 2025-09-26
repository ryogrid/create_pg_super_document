# GUCArrayAdd

## Location
[src/backend/utils/misc/guc.c:6496-6573](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/misc/guc.c#L6496-L6573)

## Overview
GUCArrayAdd adds or updates a GUC setting entry in a PostgreSQL array, creating a new array with the added/modified entry while handling parameter validation and name normalization.

## Definition

```c
struct config_generic *record;
```
## Detailed Description
GUCArrayAdd manages GUC setting arrays by adding new entries or updating existing ones. These arrays are commonly used to store configuration settings in system catalogs like pg_db_role_setting.setconfig and pg_proc.proconfig. The function provides intelligent handling of both new arrays and existing arrays.

Key operations performed:
1. **Validation**: Uses validate_option_array_item to ensure the parameter name and value are valid
2. **Name normalization**: Uses find_option to convert obsolete GUC names to their modern equivalents
3. **Array handling**: Either creates a new single-element array or modifies an existing array
4. **Duplicate detection**: Scans existing array entries to find and replace matching parameter names
5. **Memory management**: Constructs a new ArrayType with proper PostgreSQL array structure

The function handles the special case where the input array is NULL by creating a new array with a single element. For existing arrays, it searches for duplicate parameter names (matching up to and including the '=' character) and replaces them, otherwise appends the new setting.

## Parameters / Member Variables
- : Existing ArrayType containing GUC settings, or NULL to create a new array
- : Parameter name to add or update (will be normalized to canonical form)
- : Parameter value to set

## Dependencies
- Functions called/Symbols referenced:
  - [validate_option_array_item](../v/validate_option_array_item.md) (validate parameter name and value)
  - [find_option](../f/find_option.md) (locate and normalize GUC parameter names)
  - [psprintf](../p/psprintf.md) (format "name=value" string)
  - CStringGetTextDatum (convert C string to PostgreSQL text datum)
  - [array_ref](../a/array_ref.md) (extract existing array elements)
  - [array_set](../a/array_set.md) (update array element at specific position)
  - [construct_array_builtin](../c/construct_array_builtin.md) (create new single-element array)
  - ARR_ELEMTYPE/ARR_NDIM/ARR_LBOUND/ARR_DIMS (array metadata macros)
- Called from (representative examples):
  - [AlterSetting](../A/AlterSetting.md) (src/backend/catalog/pg_db_role_setting.c:118,144)
  - [update_proconfig_value](../u/update_proconfig_value.md) (src/backend/commands/functioncmds.c:660)

## Notes and Other Information
- Returns a new ArrayType - the original array is not modified in place
- Automatically handles both creation of new arrays and modification of existing arrays
- Parameter names are normalized to their canonical forms (e.g., obsolete names are updated)
- Duplicate parameter detection compares names including the '=' delimiter
- Input validation ensures only valid GUC parameters and values are accepted
- Memory allocation is handled through PostgreSQL's array construction functions
- Used primarily by ALTER ROLE/DATABASE SET and CREATE/ALTER FUNCTION commands
- The function assumes TEXT element type and 1-dimensional arrays with 1-based indexing