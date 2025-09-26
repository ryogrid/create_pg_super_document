# GUCArrayReset

## Location
[src/backend/utils/misc/guc.c:6644-6715](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/misc/guc.c#L6644-L6715)

## Overview
Resets GUC configuration entries in an array based on the current user's permission level, removing all entries that the user has permission to delete.

## Definition

```c
struct_array_builtin(&d, 1, TEXTOID);
```
## Detailed Description
GUCArrayReset selectively removes GUC parameter entries from a configuration array based on the user's privileges. For superusers, it removes all entries (returns NULL). For regular users, it only removes entries for parameters that are either PGC_USERSET (user-settable) or that the user has explicit permission to modify. The function preserves entries that the user lacks permission to change, ensuring proper access control for configuration settings.

The function iterates through each entry in the array, extracts the parameter name from the "name=value" format, and uses validate_option_array_item() to determine if the current user can modify that parameter. Entries that cannot be modified are preserved in the resulting array.

## Parameters / Member Variables
- : Input ArrayType containing GUC configuration entries; may be NULL

## Dependencies
- Functions called/Symbols referenced:
  - superuser
  - ARR_DIMS
  - array_ref
  - TextDatumGetCString
  - validate_option_array_item
  - array_set
  - construct_array_builtin
  - strchr
  - pfree
- Called from (representative examples):
  - AlterSetting
  - EmitWarningsOnPlaceholders

## Notes and Other Information
- Returns NULL if input array is NULL or if superuser (indicating complete reset)
- For non-superusers, preserves only parameters they lack permission to modify
- Uses permission checking through validate_option_array_item() with silent mode
- Temporarily modifies the parameter string by null-terminating at the '=' character
- Properly frees memory for extracted parameter strings using pfree()
- Creates a new array rather than modifying the input array in-place
- Used in role-based configuration management where different users have different modification privileges