# GUCArrayDelete

## Location
src/backend/utils/misc/guc.c: 6574 - 6643

## Overview
Deletes a specific GUC (Grand Unified Configuration) parameter entry from a configuration array by name, returning a new array without the specified entry.

## Definition


## Detailed Description
GUCArrayDelete removes a configuration parameter entry from an array of GUC settings. The function searches through the input array for entries matching the specified parameter name and creates a new array excluding the matching entry. The function handles null input arrays gracefully and validates that the parameter name is valid before attempting deletion. Configuration entries are stored in "name=value" format, and the function performs string matching on the parameter name portion.

The function normalizes obsolete GUC parameter names to their modern spellings using find_option() before performing the deletion. If the input array is null or the entry to delete is not found, appropriate return values are provided.

## Parameters / Member Variables
- : Input ArrayType containing GUC configuration entries in "name=value" format; may be NULL
- : Name of the GUC parameter to delete from the array; must not be NULL

## Dependencies
- Functions called/Symbols referenced:
  - validate_option_array_item
  - find_option  
  - ARR_DIMS
  - array_ref
  - TextDatumGetCString
  - array_set
  - construct_array_builtin
  - config_generic
- Called from (representative examples):
  - AlterSetting
  - update_proconfig_value
  - EmitWarningsOnPlaceholders

## Notes and Other Information
- Returns NULL if the input array is NULL or if the resulting array would be empty
- Validates the parameter name before deletion using validate_option_array_item()
- Normalizes obsolete GUC names to current spellings for consistent deletion
- Uses 1-based array indexing consistent with PostgreSQL array conventions
- The function creates a completely new array rather than modifying the input array in-place
- Used primarily in database role settings and function configuration management