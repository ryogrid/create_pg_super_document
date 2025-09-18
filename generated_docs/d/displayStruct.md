# displayStruct

## Location
src/backend/utils/misc/help_config.c: 74 - 86

## Overview
A filtering function that determines whether a PostgreSQL GUC configuration structure should be displayed to the user based on its visibility flags.

## Definition
```c
static bool displayStruct(mixedStruct *structToDisplay)
```

## Detailed Description
displayStruct serves as a filtering mechanism for PostgreSQL's configuration help system. It examines the flags field of a configuration variable and determines whether it should be shown to users. The function returns false for configuration variables that are marked as internal-only, not suitable for sample configurations, or not allowed in configuration files. This ensures that only user-relevant configuration options are displayed when using PostgreSQL's help functionality.

## Parameters / Member Variables
- `structToDisplay`: Pointer to a mixedStruct containing a PostgreSQL configuration variable to be evaluated for display eligibility

## Dependencies
- Functions called/Symbols referenced:
  - GUC_NO_SHOW_ALL (flag constant)
  - GUC_NOT_IN_SAMPLE (flag constant) 
  - GUC_DISALLOW_IN_FILE (flag constant)
- Types referenced:
  - mixedStruct
- Called from (representative examples):
  - [GucInfoMain](../G/GucInfoMain.md)

## Notes and Other Information
- Located in src/backend/utils/misc/help_config.c:74-86
- This is a static function, only accessible within the help_config.c file
- The function uses bitwise AND operation to check for any of the three exclusion flags
- Returns true if none of the exclusion flags are set, false otherwise
- The three flags filtered out are:
  - GUC_NO_SHOW_ALL: Parameters not shown in SHOW ALL commands
  - GUC_NOT_IN_SAMPLE: Parameters not included in sample configuration files
  - GUC_DISALLOW_IN_FILE: Parameters that cannot be set in configuration files