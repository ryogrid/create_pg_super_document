# GetPGVariable

## Location
src/backend/utils/misc/guc_funcs.c: 382 - 393

## Overview
GetPGVariable is the main entry point function for the SHOW command, determining whether to display all GUC (Grand Unified Configuration) variables or a specific configuration option based on the provided variable name.

## Definition
void GetPGVariable(const char *name, DestReceiver *dest)

## Detailed Description
This function serves as a dispatcher for SHOW commands in PostgreSQL. It performs a case-insensitive comparison of the requested variable name against the string "all" to determine the appropriate action:
- If the name matches "all", it calls ShowAllGUCConfig() to display all configuration variables
- Otherwise, it calls ShowGUCConfigOption() to show the specific requested variable

The function uses guc_name_compare() for name comparison, which implements a custom ASCII-only case-insensitive comparison that remains stable across different locale settings.

## Parameters / Member Variables
- name: The name of the configuration variable to show, or "all" to show all variables
- dest: The destination receiver where the output should be sent (typically for returning results to the client)

## Dependencies
- Functions called/Symbols referenced:
  - guc_name_compare (for case-insensitive name comparison)
  - ShowAllGUCConfig (to display all GUC variables)
  - ShowGUCConfigOption (to display a specific GUC variable)
  - DestReceiver (type for output destination)
- Called from (representative examples):
  - exec_replication_command (in replication context)
  - standard_ProcessUtility (during utility command processing)

## Notes and Other Information
- This function is the primary interface between the SQL SHOW command and the GUC system
- The comparison logic ensures that SHOW ALL works regardless of case variations
- The function delegates all actual work to specialized display functions, maintaining a clean separation of concerns
- Located in src/backend/utils/misc/guc_funcs.c:382-393