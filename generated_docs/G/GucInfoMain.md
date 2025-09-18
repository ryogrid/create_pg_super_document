# GucInfoMain

## Location
[src/backend/utils/misc/help_config.c:46-73](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/misc/help_config.c#L46-L73)

## Overview
The main entry point function that displays all PostgreSQL GUC (Grand Unified Configuration) parameters that are available to users, filtering out internal or hidden configuration options.

## Definition
```c
void GucInfoMain(void)
```

## Detailed Description
GucInfoMain serves as the primary function for the PostgreSQL configuration help utility. It initializes the GUC system, retrieves all available configuration variables, and outputs information about each one that should be displayed to users. The function filters out configuration parameters marked with specific flags (GUC_NO_SHOW_ALL, GUC_NOT_IN_SAMPLE, or GUC_DISALLOW_IN_FILE) to present only user-relevant options. After processing all variables, it terminates the program with exit(0).

## Parameters / Member Variables
This function takes no parameters.

## Dependencies
- Functions called/Symbols referenced:
  - [build_guc_variables](../b/build_guc_variables.md)
  - get_guc_variables
  - [displayStruct](../d/displayStruct.md)
  - [printMixedStruct](../p/printMixedStruct.md)
  - exit
- Types referenced:
  - config_generic
  - mixedStruct
- Called from (representative examples):
  - [main](../m/main.md) (from src/backend/main/main.c:194)

## Notes and Other Information
- Located in src/backend/utils/misc/help_config.c:46-73
- This function is the core of PostgreSQL's `--help-config` functionality
- The function processes configuration variables in a loop, casting each generic config structure to mixedStruct for unified handling
- Uses a union (mixedStruct) to handle different types of configuration parameters (boolean, integer, real, string, enum)
- The function terminates the entire program after displaying the configuration information