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
  - [superuser](../s/superuser.md)
  - ARR_DIMS
  - [array_ref](../a/array_ref.md)
  - TextDatumGetCString
  - [validate_option_array_item](../v/validate_option_array_item.md)
  - [array_set](../a/array_set.md)
  - [construct_array_builtin](../c/construct_array_builtin.md)
  - strchr
  - [pfree](../p/pfree.md)
- Called from (representative examples):
  - [AlterSetting](../A/AlterSetting.md)
  - EmitWarningsOnPlaceholders

## Notes and Other Information
- Returns NULL if input array is NULL or if superuser (indicating complete reset)
- For non-superusers, preserves only parameters they lack permission to modify
- Uses permission checking through validate_option_array_item() with silent mode
- Temporarily modifies the parameter string by null-terminating at the '=' character
- Properly frees memory for extracted parameter strings using pfree()
- Creates a new array rather than modifying the input array in-place
- Used in role-based configuration management where different users have different modification privileges

## Simplified Source

```c
ArrayType *GUCArrayReset(ArrayType *array) {
    ArrayType *newarray = NULL;
    int index = 1;

    // Return NULL if input array is NULL
    if (!array)
        return NULL;

    // Superusers can delete everything - return NULL for complete reset
    if (superuser())
        return NULL;

    // For regular users, preserve only settings they can't modify
    for (int i = 1; i <= ARR_DIMS(array)[0]; i++) {
        bool isnull;
        Datum d = array_ref(array, 1, &i, -1, -1, false, TYPALIGN_INT, &isnull);

        if (!isnull) {
            char *val = TextDatumGetCString(d);
            char *eqsgn = strchr(val, '=');
            *eqsgn = '\0';  // Temporarily null-terminate to extract parameter name

            // Skip entries the user has permission to delete
            if (validate_option_array_item(val, NULL, true)) {
                pfree(val);
                continue;
            }

            // Preserve entries the user cannot modify
            if (newarray)
                newarray = array_set(newarray, 1, &index, d, false, -1, -1, false, TYPALIGN_INT);
            else
                newarray = construct_array_builtin(&d, 1, TEXTOID);

            index++;
            pfree(val);
        }
    }

    return newarray;
}
```