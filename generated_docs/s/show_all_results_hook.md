# show_all_results_hook

## Location
[src/bin/psql/startup.c:1150-1155](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/bin/psql/startup.c#L1150-L1155)

## Overview
A variable hook function that processes changes to the SHOW_ALL_RESULTS variable in psql, controlling whether all result sets from a multi-statement query are displayed or just the last one.

## Definition

```c
static bool
show_all_results_hook(const char *newval)
```
## Detailed Description
This hook function is responsible for parsing and setting the show_all_results option when the SHOW_ALL_RESULTS variable is modified in psql. It uses the ParseVariableBool utility function to convert the string value to a boolean and store it in the pset.show_all_results field. When enabled, this setting causes psql to display results from all statements in a multi-statement query rather than just showing the result of the final statement.

## Parameters / Member Variables
- : The new value for the SHOW_ALL_RESULTS variable as a string. Should be a valid boolean representation ("on", "off", "true", "false", "1", "0", etc.).

## Dependencies
- Functions called/Symbols referenced:
  - [ParseVariableBool](../P/ParseVariableBool.md) (utility function for parsing boolean variables)
  - pset.show_all_results (global variable to store the setting)
- Called from (representative examples):
  - SetVariableHooks registration in EstablishVariableSpace

## Notes and Other Information
- This function delegates the actual parsing and error handling to ParseVariableBool
- The return value comes directly from ParseVariableBool, indicating success (true) or failure (false) of the parsing
- When enabled, SHOW_ALL_RESULTS affects how psql handles queries containing multiple statements separated by semicolons
- Paired with bool_substitute_hook which provides default value handling for boolean variables
- Located in src/bin/psql/startup.c:1150