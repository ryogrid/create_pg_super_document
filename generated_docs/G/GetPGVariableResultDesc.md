# GetPGVariableResultDesc

## Location
src/backend/utils/misc/guc_funcs.c: 394 - 427

## Overview
GetPGVariableResultDesc creates and returns a tuple descriptor that defines the structure of result tuples for SHOW commands, with different column layouts depending on whether all variables or a single variable is being requested.

## Definition
TupleDesc GetPGVariableResultDesc(const char *name)

## Detailed Description
This function builds the appropriate tuple descriptor for SHOW command results based on the requested variable name:

**For SHOW ALL (when name equals "all"):**
- Creates a 3-column tuple descriptor with TEXT columns:
  - "name": The configuration parameter name
  - "setting": The current value of the parameter
  - "description": A description of the parameter

**For specific variables:**
- Creates a 1-column tuple descriptor with a single TEXT column
- Uses the canonical form of the variable name as the column name
- Calls GetConfigOptionByName() to retrieve and validate the canonical variable name

The function uses CreateTemplateTupleDesc() to allocate the descriptor and TupleDescInitEntry() to define each column with TEXTOID type.

## Parameters / Member Variables
- name: The name of the configuration variable to describe, or "all" to describe the format for showing all variables

## Dependencies
- Functions called/Symbols referenced:
  - guc_name_compare (for case-insensitive name comparison)
  - CreateTemplateTupleDesc (to create tuple descriptor structure)
  - TupleDescInitEntry (to initialize individual column definitions)
  - GetConfigOptionByName (to get canonical variable name and validate access)
- Called from (representative examples):
  - UtilityTupleDescriptor (during utility command processing)

## Notes and Other Information
- This function provides the schema definition that corresponds to the actual data returned by GetPGVariable()
- The tuple descriptor format must match exactly what ShowAllGUCConfig() and ShowGUCConfigOption() produce
- For specific variables, the column name uses the canonical form to ensure consistency
- All columns are defined as TEXT type with unlimited length (-1) and no type modifier (0)
- Located in src/backend/utils/misc/guc_funcs.c:394-427