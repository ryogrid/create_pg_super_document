# ProcessGUCArray

## Location
[src/backend/utils/misc/guc.c:6464-6495](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/misc/guc.c#L6464-L6495)

## Overview
ProcessGUCArray applies GUC settings from a PostgreSQL array to the current session, handling configuration arrays from sources like pg_db_role_setting.setconfig and pg_proc.proconfig.

## Definition

```c
struct config_generic *record;
```
## Detailed Description
ProcessGUCArray is a high-level function that processes arrays of GUC settings stored in system catalogs such as database/role-specific settings and function-specific configurations. The function combines the parsing capabilities of TransformGUCArray with the application logic of set_config_option.

The function operates in these steps:
1. **Parse array**: Uses TransformGUCArray to convert the array into parallel lists of names and values
2. **Apply settings**: Iterates through both lists simultaneously using forboth macro
3. **Set options**: Calls set_config_option for each name-value pair with the specified context, source, and action
4. **Cleanup**: Frees the allocated memory for names, values, and lists

This function is typically used when applying stored configuration settings that have been retrieved from the system catalogs during session initialization or function execution.

## Parameters / Member Variables
- : ArrayType containing GUC settings as text elements in "name=value" format (must not be NULL)
- : GucContext specifying the configuration context (e.g., PGC_USERSET, PGC_SUSET)
- : GucSource indicating where the settings originated (e.g., PGC_S_DATABASE, PGC_S_USER)
- : GucAction specifying how to handle the settings (e.g., GUC_ACTION_SET, GUC_ACTION_LOCAL)

## Dependencies
- Functions called/Symbols referenced:
  - TransformGUCArray (parse array into name/value lists)
  - forboth (macro for iterating parallel lists)
  - set_config_option (apply individual GUC setting)
  - pfree (free individual name/value strings)
  - list_free (free list structures)
- Called from (representative examples):
  - ApplySetting (src/backend/catalog/pg_db_role_setting.c:256)
  - ProcedureCreate (src/backend/catalog/pg_proc.c:696)

## Notes and Other Information
- The array parameter must be non-NULL and contain TEXT elements
- Caller is responsible for specifying appropriate context, source, and action parameters
- Memory cleanup is handled automatically - caller doesn't need to free names/values
- Used primarily for applying stored configuration from system catalogs
- The forboth macro ensures safe iteration over parallel lists of equal length
- Function provides a convenient wrapper around the lower-level TransformGUCArray and set_config_option functions
- Settings that fail to apply will generate appropriate error messages through set_config_option