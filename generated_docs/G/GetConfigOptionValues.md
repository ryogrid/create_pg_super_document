# GetConfigOptionValues

## Location
src/backend/utils/misc/guc_funcs.c: 594 - 806

## Overview
Extracts and formats field values for a configuration parameter to display in the pg_settings system view.

## Definition


## Detailed Description
This function takes a configuration parameter structure and populates an array of string values representing all the attributes of that parameter as they should appear in the pg_settings system view. It handles the extraction of both generic attributes (name, setting, unit, group, description, etc.) and type-specific attributes (min_val, max_val, enumvals, boot_val, reset_val) for all supported PostgreSQL configuration parameter types (boolean, integer, real, string, and enum).

The function performs type-specific handling to format values appropriately:
- For boolean parameters: converts true/false to "on"/"off" strings
- For numeric parameters: formats min/max values and converts numbers to strings
- For string parameters: handles NULL values appropriately
- For enum parameters: builds a formatted list of valid options and looks up symbolic names
- For all types: manages source file information based on user privileges

## Parameters / Member Variables
- : Pointer to the generic configuration parameter structure containing the parameter's metadata and current values
- : Output array of string pointers (17 elements) to be populated with formatted parameter information

## Dependencies
- Functions called/Symbols referenced:
  - [ShowGUCOption](../S/ShowGUCOption.md)
  - get_config_unit_name
  - config_enum_get_options
  - config_enum_lookup_by_value
  - has_privs_of_role
  - [GetUserId](GetUserId.md)
  - [pstrdup](../p/pstrdup.md)
- Called from (representative examples):
  - [show_all_settings](../s/show_all_settings.md)

## Notes and Other Information
- This is a static function used internally by the GUC (Grand Unified Configuration) system
- The function populates exactly 17 string values corresponding to the columns in pg_settings
- Source file and line number information is only shown to users with appropriate privileges (ROLE_PG_READ_ALL_SETTINGS)
- The function handles all five PostgreSQL configuration parameter types: PGC_BOOL, PGC_INT, PGC_REAL, PGC_STRING, and PGC_ENUM
- Memory allocation is performed using pstrdup() for string values that need to persist beyond the function call